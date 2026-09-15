# Architecture

## Scope

This service is a **stateless, non-blocking (Spring WebFlux/Reactor)** integration
and orchestration layer between the Case Manager chatbot capability and the external
ML/AI Agent:

```
Frontend → Java Chat Orchestrator → ML Agent → Java Chat Orchestrator → SSE → Frontend
```

It validates each request, propagates tenant/case/user/correlation/trace metadata,
calls the ML Agent through a circuit breaker + bulkhead + bounded retry + declarative
timeouts, and streams the response back. It does **not** implement a chatbot UI, an ML
model, prompt engineering, or ML Agent internals — those are owned by other teams. It
does **not own or persist conversation history** — every request is self-contained,
and conversation memory (if any) is owned by the ML Agent or the frontend, never this
backend. It does **not** introduce a database, Redis, Kafka, or any other persistence
— nothing here needs one, and the brief that drove this iteration explicitly said not
to add one "unless a future explicit requirement requires it."

All ML functionality is behind
[`MlAgentClient`](../src/main/java/com/cmbotservice/mlagent/MlAgentClient.java), with
two implementations selected purely by configuration
(`@ConditionalOnProperty(ml-agent.mode)`, never a runtime `if/else`):
[`MockMlAgentClient`](../src/main/java/com/cmbotservice/mlagent/MockMlAgentClient.java)
(default) and
[`GrpcMlAgentClient`](../src/main/java/com/cmbotservice/mlagent/GrpcMlAgentClient.java)
(real, gRPC-based — service-to-service traffic between two internal services, so gRPC
was chosen over the originally integrated HTTP+SSE transport; see §6/§7). This is the
second time the thing behind `MlAgentClient` has changed (placeholder → real HTTP →
gRPC) without `ChatOrchestrationService`, `ChatController`, or the frontend-facing SSE
contract needing to change at all — proof the abstraction boundary was drawn in the
right place.

## 1. Spring MVC vs WebFlux — superseded decision

An earlier iteration of this service ran on Spring MVC + Java 21 virtual threads +
blocking `SseEmitter`, a deliberate and defensible choice for a moderate-traffic POC:
virtual threads make blocking cheap, so thread-per-connection stops being a scaling
problem. That reasoning is retired here, not because it was wrong for what it
addressed, but because the requirement changed to **very high traffic and a very
large number of concurrent long-lived SSE connections**, with production-grade
resilience composed *declaratively*. Virtual threads solve "blocking is cheap"; they
do not give you a `.timeout()` operator, a `.retryWhen()` that only retries before the
first byte, backpressure that's real rather than assumed, or cancellation that
propagates through every layer automatically. Reactor's operator model does all four,
and composes them in one place (`ChatOrchestrationService`) instead of scattering
watchdogs, executors, and manual cancellation flags across the codebase — which is
what the MVC version had to do by hand for each of these concerns individually.

Nothing about the service's actual responsibility changed. This is a technology and
hardening upgrade to the same thin orchestrator, refactored incrementally — not a
rewrite from zero.

## 2. Request/Response Flow

```
Frontend (curl/Swagger)
   │  POST /api/v1/chat/messages   (Accept: text/event-stream)
   │  { tenantId, caseId, conversationId?, continuation?, requestId?, endUserId?, message }
   ▼
ChatController
   → Bean Validation (WebExchangeBindException → 400, pre-stream — the only way a
   │   request fails before the SSE response commits at 200)
   → resolves RequestContext (tenant/case/user/correlationId)
   → returns Flux<ServerSentEvent<Object>> — Spring subscribes and writes as elements arrive
   ▼
ChatOrchestrationService (fully reactive — no thread, no blocking call, anywhere here)
   → message-length check (runtime-configurable ceiling)
   → builds MlAgentRequest (surface hardcoded "case_manager"; continuation/conversationId
   │   both echoed through verbatim, never chosen between)
   → mlAgentClient.streamResponse(...)
       .transformDeferred(CircuitBreakerOperator)
       .transformDeferred(BulkheadOperator)
       .timeout(firstResponseTimeout, evt -> idleTimeout)
       .retryWhen(backoff, filtered to retryable-and-nothing-streamed-yet)
       .transform(withTotalDeadline)
   → maps each MlAgentStreamEvent → ChatSseEvent → ServerSentEvent
   → onErrorResume: any failure past this point becomes an `error` event, not an HTTP status
   ▼
MockMlAgentClient / GrpcMlAgentClient (impl of MlAgentClient)
   → streams Started → Token* → Payload → Done — conversationId/continuation are
     revealed only on Done, never before (see the callout below)
   → (real client) never buffers the full response — a straight Flux, gRPC → Netty
```

**The one finding that reshaped this design:** the real ML Agent (originally
`POST /v1/chat` over HTTP+SSE, now the same contract's `Chat` RPC over gRPC — see §6,
built by Thoughtful Labs) never reveals a conversation identifier until its final
`done` event — there is no early "here's your conversation id" moment. An earlier
placeholder version of this contract assumed the agent assigned one up front and
handed it back on `Started`; it doesn't, so `MlAgentStreamEvent.Started` carries no
fields at all, and `conversationId`/`continuation` only become authoritative on
`MlAgentStreamEvent.Done` → `StreamCompleteEvent`. Every SSE event before
`stream-complete` echoes whatever the *request* supplied (or blank for a new
conversation) — documented explicitly on `StreamStartEvent` as "not authoritative."
This finding predates and is independent of the HTTP→gRPC transport swap — it's a
property of the ML Agent's own contract, not of either transport.

### The real ML Agent request/response contract

Originally documented as a JSON body over `POST /v1/chat` + SSE; now the same fields,
same semantics, carried as a protobuf message over gRPC's server-streaming `Chat` RPC
(`src/main/proto/chat_agent.proto` — see §6 for the live schema). Shown here as JSON
since that's how the contract was first specified and is still the easiest way to read
it; the field names below map 1:1 to the `.proto` message fields (mostly identical,
`camelCase` JSON ↔ `snake_case` proto).

Request body (originally `POST /v1/chat`, now the `ChatRequest` proto message):

```json
{
  "continuation": "v1.k3.eyJlbmMi...",
  "conversationId": null,
  "history": [
    { "role": "user", "content": "Summarise this case" },
    { "role": "assistant", "content": "..." }
  ],
  "surface": "case_manager",
  "message": "Summarise this case",
  "context": { "caseId": "1234", "endUserId": "<sha256-base64>" },
  "tenantId": "shcuat1b",
  "options": { "includeResolutions": true }
}
```

`history`, `continuation`, and `conversationId` are the contract's three resumption
mechanisms (precedence `history > continuation > conversationId`); this service
forwards all three exactly as the caller (`ChatRequest`) supplied them and never
chooses between them — it doesn't assemble, store, or interpret any of the three,
it just echoes back whatever `continuation`/`conversationId` the previous `done`
event returned, and passes `history` straight through untouched. This is still a
fully stateless pass-through: the caller (frontend/BFF), not this service, owns
remembering and resending the transcript, exactly as it already owns
`continuation`/`conversationId`. **The exact per-turn shape (`role`/`content`) is
this backend's best-effort assumption** — the real contract documents `history`
only as "an explicit transcript," with no field-level schema given — see
`src/main/proto/chat_agent.proto`'s `HistoryTurn` message and README.md "Known
limitations". `contextToken` (the product/prod-auth path) is still deliberately
never sent — no real auth/JWT infrastructure exists behind it yet (documented gap,
not a bug).

`context.endUserId` is documented as a SHA-256 hash, base64-encoded — **who computes
that hash is not specified by the contract and is not yet resolved.** This backend
forwards whatever `ChatRequest.endUserId` supplies untouched (no hashing logic exists
here); if the frontend/caller is expected to send a raw value for this backend to
hash before forwarding, that's a gap, not an assumption we've silently made — flagged
explicitly rather than guessed at.

Six SSE event types:

| Event | Payload | Cardinality |
|---|---|---|
| `token` | `{ delta }` | many — streaming answer text |
| `tool_call` | `{ id, name, args }` | 0..n — "rendered in the sandbox trace, logged in product" |
| `tool_result` | `{ id, ms, rowCount, ok }` | one per `tool_call` |
| `payload` | structured Case Manager analysis (below) | exactly one, before `done` |
| `done` | `{ conversationId, continuation, latencyMs, tokensIn, tokensOut }` | one, terminal |
| `error` | `{ code, message, retryable }` | terminal |

`payload` (Case Manager surface):

```json
{
  "answer": "...",
  "summary": {
    "narrative": "...",
    "keySignals": [{ "signal": "...", "severity": "high", "citations": ["7ff7-..._TRX"] }],
    "entities": [{ "type": "ip", "value": "100.100.100.100", "events": ["7ff7-..._TRX"] }],
    "timeline": [{ "at": "2022-08-10T14:14:02Z", "eventId": "7ff7-..._TRX", "what": "..." }]
  },
  "suggestedResolution": { "mark": "SUSPECTED_FRAUD", "confidence": "medium", "rationale": "..." },
  "citations": [{ "id": "7ff7-..._TRX", "source": "AgenticGetCase.events[].decision", "fields": ["ri..."] }]
}
```

Two invariants `GrpcMlAgentClient` enforces rather than trusts blindly (§16): every
`keySignal` must carry ≥1 citation ("a signal without a resolvable citation is a
defect, not a soft failure"), and `suggestedResolution.mark` is always one of the
resolution enum names `CONFIRMED_FRAUD | SUSPECTED_FRAUD | CONFIRMED_GENUINE |
ASSUMED_GENUINE | UNKNOWN` ("the agent never invents one"). `ANY` is documented as
filter-only and must never be emitted, so its presence is treated as a contract
violation, not accepted. The *single-character* codes (`F S G A U Y B T`) are a
different system's (`APP_EVENT_UPDATE.CUSTOM_MARK`) internal representation and never
appear on this interface — an important correction from an earlier revision of this
doc, which had (incorrectly, per the now-clarified contract) assumed `mark` used those
single-character codes directly, with `X`/`C` gated behind a tenant flag. That
tenant-flag reasoning no longer applies; the known set above is fixed and unconditional.

The `suggestedResolution` example above also no longer shows a `label` field
alongside `mark`/`confidence`/`rationale` — **unclear whether `label` was dropped from
the contract or just omitted from this particular example.** `CaseSummaryPayload`
still carries it (unvalidated, forwarded as-is if present, blank if not) rather than
removing it outright, since deleting a field on a guess risks silently dropping real
data if the agent still sends it.

Errors — one unified code space, used either as the gRPC status of the initial `Chat`
call (pre-stream — mapped from the closest-matching `Status.Code`, since gRPC has no
literal "401") or as the terminal `error` event's `code` (in-stream, sent verbatim by
the agent as these exact strings):

| Code | Meaning | Retryable |
|---|---|---|
| 401 | Bad/expired service credential | no |
| 403 | Tenant not permitted for this caller | no |
| 404 | Case not found in that tenant | no |
| 422 | Malformed request / missing required context | no |
| 429 | Rate limited | yes |
| 503 | Agentic API or model endpoint unavailable | yes |
| 4221 | Continuation expired — start a new conversation | no |
| 4222 | Continuation undecryptable/tampered | no |

**Cancellation:** a client disconnect cancels the subscription; Reactor propagates
that cancellation upstream through every operator automatically (`.timeout()`'s
internal timer, `delayElements`' pending timer, the gRPC `ClientCallStreamObserver`'s
`cancel(...)`, actually tearing down the underlying HTTP/2 call — see §6) — verified
live: disconnecting mid-`trigger:slow` logs `SSE_CLIENT_CANCELLED` at INFO with no
wasted downstream work, no ERROR-level noise.

## 3. Component Diagram

```
                       ┌──────────────────────┐
   Frontend      ───▶  │  ChatController        │
   (curl/Swagger)      │  (WebFlux)             │
                       └──────────┬─────────────┘
                                  │ ChatRequest
                       ┌──────────▼─────────────┐
                       │  ChatOrchestrationService│◀── RequestContext (Reactor Context, not ThreadLocal)
                       │  + ChatMetrics            │
                       └──────────┬─────────────┘
                                  │
                  ┌───────────────┼────────────────┐
                  │  CircuitBreaker │  Bulkhead      │  (resilience4j, from ResilienceConfig)
                  └───────────────┼────────────────┘
                                  │
                       ┌──────────▼─────────────┐
                       │   MlAgentClient (I/F)    │
                       │  ── MockMlAgentClient     │  ── GrpcMlAgentClient (gRPC ManagedChannel)
                       └──────────┬─────────────┘
                                  │
                       ┌──────────▼─────────────┐
                       │  Flux<ServerSentEvent>   │──▶ frontend (SSE stream)
                       └──────────────────────────┘

Cross-cutting: CorrelationIdFilter (WebFilter) + MdcContext (Reactor Context ↔ MDC
bridge via Micrometer), GlobalExceptionHandler, OpenApiConfig, Actuator/Micrometer/Tracing.

No repository/persistence layer anywhere in this diagram — there is none.
```

## 4. API Surface & the pre-stream/in-stream error boundary

One endpoint: `POST /api/v1/chat/messages`. `tenantId`/`caseId`/`conversationId`/
`continuation`/`requestId`/`endUserId` travel in the request body — there is no
backend-owned resource to nest a path segment under. `conversationId`/`continuation`
are both optional (omit both to start a new conversation, send back either/both from
a prior `stream-complete` to continue one); this backend never stores or interprets
either.

The one architectural line that matters here: **Bean Validation is the only thing
that can produce a non-200 HTTP status.** Once `ChatOrchestrationService` returns its
`Flux`, Spring commits the response at 200/`text/event-stream` as soon as it starts
writing — there is no way to change the status after that. So every failure that can
occur once the ML Agent call has actually begun (timeout, circuit-breaker-open,
bulkhead-full, retry-exhausted, malformed response) is deliberately funneled through
one `.onErrorResume(...)` into an `error` SSE event instead, on the same stream, with
the same shape as a normal completion. This is documented explicitly on the
`@Operation` Swagger annotation so a frontend integrator doesn't go looking for a 503.

## 5. SSE Event Contract

`Content-Type: text/event-stream`. Each event has an `id:` (sequence number, assigned
uniformly across all five types via one `Flux#index()` over the fully-assembled event
stream — see [`SseEvents`](../src/main/java/com/cmbotservice/sse/SseEvents.java)),
`event:` (type), and `data:` (JSON):

| Event | Payload |
|---|---|
| `stream-start` | `{ conversationId, messageId, timestamp }` — `conversationId` is an unconfirmed echo, not authoritative (see the §2 callout) |
| `message` (0+) | `{ conversationId, messageId, sequence, content, timestamp }` — the ML Agent's `token`/`delta`, translated |
| `payload` (exactly 1, before `stream-complete`) | `{ conversationId, messageId, payload, timestamp }` — the ML Agent's structured `payload` event, translated directly (its shape already matches what the case manager UI needs) |
| `stream-complete` | `{ conversationId, continuation, messageId, totalChunks, timestamp }` — the only authoritative source of both identifiers |
| `error` | `{ conversationId, messageId, errorCode, errorMessage, timestamp }` |

Exactly one of `stream-complete`/`error` terminates every stream. The five payload
records implement a sealed
[`ChatSseEvent`](../src/main/java/com/cmbotservice/sse/ChatSseEvent.java) interface —
a genuine Java 21 win here: the switch that builds the outbound `ServerSentEvent` is
compiler-checked exhaustive, with no `default` branch to silently swallow a future
sixth variant. `errorCode` is one of `ML_AGENT_TIMEOUT`, `ML_AGENT_UNAVAILABLE`,
`ML_AGENT_ERROR`, `CONCURRENCY_LIMIT_REACHED` (bulkhead full *or* circuit open — both
reject near-instantly and mean the same thing to a client: try later),
`CONTINUATION_EXPIRED` (the ML Agent's 4221/4222 — discard the stored
`conversationId`/`continuation` and start over), `NOT_FOUND`/`VALIDATION_ERROR`/
`INTERNAL_ERROR` (from an explicit `MlAgentRejectedException`, carrying whichever code
actually fits its underlying 401/403/404/422). `errorMessage` is always a fixed,
generic, client-safe string — never an exception message, stack trace, or gRPC/
hostname detail.

The ML Agent's own `tool_call`/`tool_result` events are consumed and logged (DEBUG)
by `GrpcMlAgentClient`/`MockMlAgentClient` directly and **never** become a domain
event or reach this outbound contract at all — per the real contract's own note that
they're "rendered in the sandbox trace, logged in product," and this service is the
product, not the sandbox.

## 6. `MlAgentClient` — the reactive contract

```java
public interface MlAgentClient {
    Flux<MlAgentStreamEvent> streamResponse(MlAgentRequest request);
}
```

`MlAgentStreamEvent` is a **sealed interface** — `Started()` (no fields; the agent
reveals nothing at start), `Token(delta, sequence)`, `Payload(CaseSummaryPayload)`,
`Done(conversationId, continuation, latencyMs, tokensIn, tokensOut)`. Errors are
deliberately **not** a variant here — they flow through the `Flux`'s native error
channel, which is what lets `.timeout()`, `.retryWhen()`, and the resilience4j
operators compose declaratively in `ChatOrchestrationService` instead of a
hand-rolled state machine (the old callback-interface design this replaced needed
exactly that). `tool_call`/`tool_result` are consumed and logged directly by each
`MlAgentClient` implementation and never become a fifth variant at all (§5).

**`MockMlAgentClient`** — no threads, no polling loops, nothing that could leak:
`Flux.concat(Mono.just(Started), tokens.delayElements(delay).index(...), Mono.just(Payload(...)), Mono.just(Done(...)))`
for success/slow (the mock fabricates a `CaseSummaryPayload` satisfying both
`GrpcMlAgentClient` validations, so this path is exercisable without a real agent);
`Flux.error(...)` after `Started` for `trigger:error`/`trigger:rejected`/
`trigger:continuation-expired`; `Flux.just(Started, Done(...))` for empty (echoing or
fabricating `conversationId`/`continuation`, same logic real Done always uses);
`Flux.concat(Mono.just(Started), Mono.never())` for timeout — the orchestrator's own
timeout operator is what ends that one, not the mock. `delayElements` natively
respects cancellation, so a disconnect mid-slow-stream stops everything downstream for
free. Entirely transport-agnostic — this class never changed across either transport
swap (placeholder → HTTP → gRPC).

**`GrpcMlAgentClient`** — built from the shared `ManagedChannel`/`ChatAgentStub` (§7),
calls the ML Agent's `Chat` server-streaming RPC
(`src/main/proto/chat_agent.proto`). grpc-java's generated async stub is
callback-based (`StreamObserver`), not `Flux`-based, so `grpcEventFlux` bridges the two
manually via `Flux.create` plus grpc-java's own manual flow-control API
(`ClientCallStreamObserver#disableAutoInboundFlowControl()`/`request(n)`) — giving real
backpressure without pulling in a third-party reactive-grpc codegen plugin. One sharp
edge worth documenting explicitly since it cost real debugging time: `request()`/
`cancel()` **cannot** be called synchronously inside `ClientResponseObserver#beforeStart(...)`
— grpc-java throws `IllegalStateException: Not started`, because `beforeStart` runs
*before* the underlying `ClientCall.start()`. The fix is to stash the
`ClientCallStreamObserver` reference in `beforeStart` and only wire
`sink.onRequest(...)`/`sink.onCancel(...)` to it *after* the `stub.chat(...)` call
returns (which is exactly when `start()` has finished) — see `GrpcMlAgentClient#grpcEventFlux`.

Never buffers the full response (no `collectList()`/`.block()` anywhere — proven by
the in-process gRPC tests, not just claimed), and validates every `payload` element
via [`CaseSummaryPayloadValidator`](../src/main/java/com/cmbotservice/mlagent/CaseSummaryPayloadValidator.java)
(§16, extracted into its own class since it's pure domain-object validation with
nothing transport-specific about it) before it becomes a domain event. Translates the
unified error-code space (§2) into `MlAgentRejectedException`/
`MlAgentContinuationExpiredException`/`MlAgentCommunicationException` both pre-stream
(the initial call's gRPC `Status.Code`) and in-stream (the terminal `error` event's
`code` string, read directly off the proto field — unchanged mapping logic from the
original HTTP integration).

**No client-side gRPC deadline is set.** `ChatOrchestrationService`'s existing
first-response/idle `.timeout()` operator (§8) remains the one, client-agnostic
timeout authority — a second, competing deadline at the gRPC-stub layer would just be
a redundant knob measuring the same thing differently.

## 7. gRPC Channel Configuration

[`GrpcChannelConfig`](../src/main/java/com/cmbotservice/config/GrpcChannelConfig.java)
builds **one** shared `ManagedChannel` bean (never per-request,
`NettyChannelBuilder.forAddress(host, port)`) and one `ChatAgentGrpc.ChatAgentStub`
bean from it. Uses **`grpc-netty-shaded`**, not plain `grpc-netty`, specifically so
this channel's own Netty usage (relocated under
`io.grpc.netty.shaded.io.netty.*`) can never collide with the reactor-netty version
WebFlux/the HTTP server already puts on the classpath — two independent Netty
instances that happen to coexist, rather than one shared (and potentially
version-mismatched) one. Plaintext only (no TLS) — internal service-to-service
traffic, and TLS material is infrastructure/secrets-management, explicitly out of
scope here (same stance already taken for the tracing exporter, §14). Only built in
`ml-agent.mode: grpc` (the mock has no network phase to open a channel for). All
values come from typed, `@Validated`
[`MlAgentProperties`](../src/main/java/com/cmbotservice/config/MlAgentProperties.java)
— a missing/invalid mandatory value fails startup, not a request.

**Build-time codegen**: `src/main/proto/chat_agent.proto` is compiled into
`ChatAgentGrpc`/message classes by `protobuf-maven-plugin` (+ the `os-maven-plugin`
build extension, which resolves the right `protoc`/`protoc-gen-grpc-java` native
binary per OS — verified working on Windows) bound to `generate-sources`, so the
generated types are on the classpath before `GrpcMlAgentClient` compiles against
them. One real gotcha hit and fixed here: **Spring Boot 4.1.1's own dependency
management already pins a specific `io.grpc`/`protobuf-java` version** (for its own
observability/OTLP support) — declaring an explicit, different `<version>` on these
dependencies in `pom.xml` caused a split-version classpath (some `io.grpc` artifacts
at one version, others at another) and a runtime `AbstractMethodError`. Fixed by
*not* pinning a version on any `io.grpc`/`protobuf-java` dependency at all, letting
Spring's own managed version apply uniformly — `grpc.version`/`protobuf.version` in
`pom.xml` now exist only to keep the `protoc`/`protoc-gen-grpc-java` **plugin**
artifacts (not managed by Spring, since they're build tooling, not a project
dependency) aligned with whatever version Spring ends up resolving. Also needed
`javax.annotation:javax.annotation-api` explicitly — the generated code references
`javax.annotation.Generated`, which was removed from the JDK itself in Java 9+.

**Tracing gap introduced by this swap, documented rather than silently accepted:** the
previous HTTP integration's ML Agent call got its own Micrometer trace span for free,
because it was built from Boot's *observation-instrumented* `WebClient.Builder`. The
gRPC `ManagedChannel` here has no equivalent automatic instrumentation wired up — a
plain `io.grpc.ClientInterceptor` bridging into Micrometer Observation would be needed
to restore that span, and building one was out of scope for this transport swap. The
frontend-facing request still gets its own span (unaffected, that's server-side
WebFlux instrumentation); what's currently missing is specifically the *outbound* ML
Agent call's own child span. See README "Known limitations."

## 8. Timeout Strategy

No connect/response-header timeout knob exists at the gRPC layer — no client-side
gRPC deadline is set anywhere in `GrpcMlAgentClient` at all (a deliberate choice, §6),
so the timeouts below are the complete story, not one layer of several:

- **First-response timeout** + **idle-stream timeout** — one Reactor operator, two
  independently configurable durations, via the companion-publisher overload:
  `flux.timeout(Mono.delay(firstResponseTimeout), evt -> Mono.delay(idleTimeout))`.
  Reactor's own `TimeoutException` is mapped to our `MlAgentTimeoutException` via
  `.onErrorMap(TimeoutException.class, ...)`.
- **Total request timeout** — Reactor has no built-in "absolute deadline regardless of
  activity" operator (`.timeout()` is always gap-based, and would never trip against a
  source that keeps actively emitting). Implemented as a small, explicitly-commented
  helper, `withTotalDeadline(Flux, Duration)`: track whether the source reached a
  terminal signal via `doOnComplete`/`doOnError` before a `.take(Duration)` cut it
  off, and synthesize an `MlAgentTimeoutException` only if it didn't. This is the one
  genuinely non-obvious piece of Reactor composition in the service, verified by a
  dedicated test (`totalDeadline_stopsAStreamThatNeverCompletesEvenWhileActivelyEmitting`)
  using a source that emits every 20ms forever.

## 9. Circuit Breaker, Bulkhead, Retry — composition and order

[`ResilienceProperties`](../src/main/java/com/cmbotservice/config/ResilienceProperties.java)
(`resilience.circuit-breaker.*`/`.bulkhead.*`/`.retry.*`) drives
[`ResilienceConfig`](../src/main/java/com/cmbotservice/config/ResilienceConfig.java),
which registers one named ("mlAgent") `CircuitBreaker`/`Bulkhead` instance from the
auto-configured (otherwise-empty) registries `resilience4j-spring-boot4` provides —
that starter's own autoconfiguration binds `/actuator/circuitbreakers`, the
circuit-breaker health indicator, and Micrometer metrics
(`resilience4j_circuitbreaker_state`, `resilience4j_bulkhead_*`) for **every** instance
in these registries automatically, so no manual metrics-binding code is needed (an
earlier version of this file had some — removed after a duplicate-registration warning
in tests revealed it was redundant).

Composition in `ChatOrchestrationService` (order matters — each retry must
re-acquire a bulkhead permit and re-check the breaker, which works because
`retryWhen` re-subscribes the whole upstream chain, including the CB/Bulkhead
operators, on every attempt):

```java
mlAgentClient.streamResponse(mlRequest)
    .doOnNext(evt -> firstEventSeen.set(true))          // must run before retryWhen
    .transformDeferred(CircuitBreakerOperator.of(cb))
    .transformDeferred(BulkheadOperator.of(bulkhead))
    .timeout(Mono.delay(firstResponseTimeout), evt -> Mono.delay(idleTimeout))
    .onErrorMap(TimeoutException.class, e -> new MlAgentTimeoutException(...))
// wrapped in Flux.defer(...) for the ML_REQUEST_STARTED log, then:
    .retryWhen(Retry.backoff(maxAttempts, initialBackoff)
        .maxBackoff(maxBackoff).jitter(jitterFactor)
        .filter(ex -> isRetryable(ex) && !firstEventSeen.get())
        .doBeforeRetry(sig -> log "ML_REQUEST_RETRY")
        .onRetryExhaustedThrow((spec, sig) -> sig.failure()))  // see note below
    .transform(flux -> withTotalDeadline(flux, maxStreamDuration))
```

Retry uses **Reactor Core's own** `reactor.util.retry.Retry.backoff(...)` (built-in
exponential backoff + jitter) rather than resilience4j's retry module — resilience4j's
`Retry` has no concept of "has this Flux emitted anything yet," and that's the one
rule that actually matters here: never retry once partial content has already
streamed to the frontend. Tracking it via a plain `AtomicBoolean` set in `doOnNext`,
checked in the retry filter, is simpler than forcing a second retry framework to do
something it isn't shaped for.

`isRetryable(Throwable)`: `MlAgentUnavailableException` (connection-level) and
`MlAgentCommunicationException` (reached the agent, got told something transient went
wrong — its own 429/503) → retryable. `MlAgentMalformedResponseException`,
`MlAgentTimeoutException`, `MlAgentRejectedException` (401/403/404/422 — the agent
understood the request and explicitly said no), `MlAgentContinuationExpiredException`
(4221/4222 — retrying with the same bad continuation just fails again),
`BulkheadFullException`, `CallNotPermittedException` → not retryable. Timeout is
deliberately non-retryable by default — retrying an agent that's already timing out
compounds load exactly when the circuit breaker should be doing the protecting
instead; the brief doesn't classify timeout either way, so this is a considered
default, called out explicitly rather than left implicit.

**A real bug found and fixed during testing:** Reactor's `Retry.backoff` wraps the
final failure in its own `RetryExhaustedException` by default once attempts run out —
which silently broke every downstream `instanceof`-based classification (error
mapping, metrics, log event names) the moment retries were actually exhausted in a
test. Fixed via `.onRetryExhaustedThrow((spec, signal) -> signal.failure())`, which
propagates the original exception directly. Worth calling out because it's exactly
the kind of thing that looks correct in isolation and only breaks under a specific,
easy-to-miss condition (retries actually running out) — which is why the test that
caught it (`retriesBeforeFirstEvent_...`) drives real exhaustion rather than mocking
around it.

Bulkhead-full and circuit-open reject essentially instantly (a synchronous check at
subscription time) — verified live: forcing the breaker open via repeated
`trigger:error`, the very next request gets an immediate `error` event
(`CONCURRENCY_LIMIT_REACHED`) with the mock never actually invoked again, and
`/actuator/health/readiness` stays UP throughout (§14).

## 10. Cancellation and Backpressure

Cancellation is a first-class Reactor concept: a client disconnect cancels the
subscription, and Reactor propagates that cancellation upstream through every
operator automatically. Verified live — killing a `curl` mid-`trigger:slow` logs
`SSE_CLIENT_CANCELLED` at INFO with no further chunks emitted and no ERROR-level
noise (the underlying `reactor.netty.channel.AbortedException`, if it surfaces at all
instead of a clean cancel signal, is filtered via `.onErrorResume(AbortedException.class, ...)`
near the end of the pipeline, same principle as the earlier MVC version's fix for
`AsyncRequestNotUsableException` — never log a routine disconnect as a server failure).

Backpressure: this is a genuinely end-to-end reactive pipeline — Reactor demand is
translated into gRPC's own manual flow-control `request(n)` calls (§6), which flow
through HTTP/2 all the way to the real ML Agent; the mock's `delayElements`/
`Flux.concat` construction only ever produces what's been requested. That is the
backpressure *strategy* to document, not a buffer size to tune: there is no
`Sinks.many()` or unbounded queue anywhere in this pipeline, confirmed by inspection
and called out explicitly since that's the one place these guarantees quietly break if
introduced carelessly.

## 11. Correlation ID / MDC / Tenant Safety in a Reactive App

[`CorrelationIdFilter`](../src/main/java/com/cmbotservice/context/CorrelationIdFilter.java)
is a `WebFilter`: accept-or-generate the correlation ID, echo it on the response
header, and write it into the Reactor `Context` via `.contextWrite(...)` around
`chain.filter(exchange)`. `tenantId`/`caseId`/`conversationId` are added to the same
`Context` once the request body is parsed, at the top of
`ChatOrchestrationService.streamMessage`.

Getting those `Context` values to actually show up in MDC on whatever thread happens
to be running at log time — without an unsafe shared `ThreadLocal` — is exactly what
Micrometer's `ContextRegistry`/`ThreadLocalAccessor` SPI exists for.
[`MdcContext`](../src/main/java/com/cmbotservice/context/MdcContext.java) registers
four `ThreadLocalAccessor<String>` instances at startup (`correlationId`/`tenantId`/
`caseId`/`conversationId`), and Reactor restores each one into MDC around every
operator boundary, scoped correctly per-subscription — *provided* automatic context
propagation is actually switched on.

**That last part was a real bug, not an assumption that happened to hold.** The
original version of this section claimed Spring Boot enables
`Hooks.enableAutomaticContextPropagation()` automatically once `context-propagation`
is on the classpath (pulled in transitively by `micrometer-tracing-bridge-otel`).
That turned out not to be happening: every registered key — `tenantId`, `caseId`,
`conversationId` — showed up blank in every log line across the entire reactive
migration, and it went unnoticed because verification focused on SSE content,
actuator endpoints, and cancellation/circuit-breaker behavior rather than scrutinizing
the MDC fields in the log output themselves. It surfaced only once the log output was
specifically checked end-to-end. Fixed by calling
`Hooks.enableAutomaticContextPropagation()` explicitly in `MdcContext`'s
`@PostConstruct`, right next to the accessor registration, so the two can't drift out
of sync again — verified live afterward: `tenantId`/`caseId` (and `correlationId`,
which worked before since it's also readable directly off the exchange, independent of
this mechanism) all populate correctly.

Two related gaps remain, **not** fixed by the above, called out explicitly rather than
implied fixed (see README "Known limitations"): `conversationId` in MDC stays blank
for a whole request even after the ML Agent resolves one, since nothing re-stamps MDC
after the value becomes known mid-stream (the SSE payloads themselves are still
correct — this is a log-context-only gap); and `traceId`/`spanId` still show blank
despite `http.server.requests` metrics confirming request observations are genuinely
being created — so Micrometer Tracing's own MDC bridging has a separate, still-open
problem, not yet root-caused.

**No separate client-owned "chat window" tracing token exists in this contract.** An
earlier iteration added one (`X-Window-Id`, a client-generated ID meant to stay stable
across a `conversationId` reset) — it was removed as unnecessary once the ML Agent
contract stabilized: `conversationId`/`continuation` already identify one
conversation thread, and `correlationId` already identifies one request, so there was
no gap left for a third identifier to fill. Log/monitoring correlation for one chatbot
interaction is fully covered by `correlationId`/`tenantId`/`caseId`/`conversationId`
alone.

This is also the tenant-safety story: `RequestContext` is passed as an explicit
parameter through every service method (never ambient/thread-local), and Reactor
`Context` is inherently per-subscription — there is no shared mutable state for one
request's tenant/case/conversation identifiers to leak into another's, even under
concurrency, even by accident.

## 12. Logging

SLF4J + Logback, color-coded console pattern (kept from the earlier iteration — see
README). MDC keys `correlationId`/`tenantId`/`caseId`/`conversationId`/`traceId`/
`spanId` appear on every log line for one chatbot interaction (§11). Logical event
names (all grep-able, see README): `CHAT_REQUEST_RECEIVED`, `ML_REQUEST_STARTED`,
`ML_STREAM_STARTED`, `ML_STREAM_COMPLETED`, `ML_REQUEST_TIMEOUT`, `ML_REQUEST_FAILED`,
`ML_REQUEST_RETRY`, `CIRCUIT_BREAKER_OPEN`, `CONCURRENCY_LIMIT_REACHED`,
`SSE_CLIENT_CANCELLED`. Levels: INFO for lifecycle, WARN for timeout/retry/rejection,
ERROR only for a genuine unexpected ML Agent failure. Never logged above DEBUG: full
prompt/response content (`LogSanitizer.preview` truncates to ~40 chars), auth headers,
stack traces in responses.

## 13. Metrics

[`ChatMetrics`](../src/main/java/com/cmbotservice/service/ChatMetrics.java) is the
**only** class allowed to touch `MeterRegistry` directly — every method takes a
fixed, low-cardinality argument, so it's structurally impossible for a future change
to accidentally tag a metric with `conversationId`/`caseId`/`userId` (the brief's own
high-cardinality warning becomes a compile-time-enforced boundary, not just a
convention). See README for the full metric list; circuit breaker/bulkhead metrics
come from resilience4j's own binders (§9), not from this class.

## 14. Tracing & Health/Readiness

`micrometer-tracing-bridge-otel` gives every incoming frontend request a real span
(server-side WebFlux instrumentation, unaffected by the ML Agent transport) —
confirmed indirectly via `http.server.requests` metrics in `/actuator/prometheus`,
which only exist because an observation is being created per request. **The ML Agent
call itself no longer automatically gets its own child span** — see §7's "tracing gap"
callout: the previous WebClient-based integration got this for free from Boot's
observation-instrumented `WebClient.Builder`; the gRPC `ManagedChannel` has no
equivalent instrumentation wired up, and adding a Micrometer-Observation-aware
`ClientInterceptor` was out of scope for this transport swap. No exporter is
configured either way — that's infrastructure, explicitly out of scope for this task.
**Also not yet working, a separate and still-open problem:** the frontend request's
own trace/span IDs are not currently reaching MDC/logs — see §11's note on this; it's
unrelated to the tracing gap above (which is about a *missing span*, not a missing MDC
bridge for an existing one).

`management.endpoint.health.group.readiness.include: readinessState` explicitly
excludes the circuit-breaker health indicator from the readiness group — verified
live: forcing the "mlAgent" circuit breaker OPEN leaves `/actuator/health/readiness`
reporting UP throughout. A downstream ML Agent outage is handled through the circuit
breaker, `error` events, and metrics — not by removing an otherwise-healthy instance
from rotation. The indicator still contributes to the plain `/actuator/health`
aggregate, which is useful signal there.

`server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase` — Reactor
Netty's graceful shutdown (stop accepting new connections, let in-flight
requests/streams finish up to the timeout, then close) is built into Boot for this
server type; pure configuration, no custom lifecycle code.

## 15. Validation, Sizing, DTO Boundaries

`ChatRequest` (frontend-facing): `@NotBlank`/`@Size`/`@Pattern` on `tenantId`/`caseId`/
`conversationId` (identifier charset + length hardening), `@Size(max=4000)` hard
ceiling on `message` plus a separately-configurable, runtime-checked
`chat.max-message-length` ceiling (belt-and-suspenders: the annotation is the
absolute limit this API will ever accept, the property lets ops tighten it further
without a redeploy). `requestId` optional, propagated to `MlAgentRequest`, logged, not
deduplicated anywhere (no store to dedupe against — by design). `history` is capped at
50 turns (`@Size`) with `@Valid` cascading into each `HistoryTurn` (`role`/`content`
both `@NotBlank`, `content` capped at 4000 chars like `message`) — bounds payload size
without this service ever inspecting a turn's content.

`spring.codec.max-in-memory-size` bounds the server-side request body; a separately
configured limit on the gRPC channel (`grpc-max-inbound-message-size`, §7) bounds its
response buffering — two different knobs for two different directions. No speculative
`metadata` map was added
to the request contract just to have size-limit annotations to attach to it — nothing
in the current contract needs one.

DTO boundary: `ChatRequest` (frontend) → `MlAgentRequest` (ML Agent, built explicitly
by `ChatOrchestrationServiceImpl`, never the same object) → `MlAgentStreamEvent` (ML
Agent's response shape) → `ChatSseEvent`/`ServerSentEvent` (frontend again). Four
distinct types, one deliberate mapping step at each boundary — the frontend is never
exposed to the ML Agent's contract directly, so ML Agent contract changes are
absorbed here, not propagated.

## 16. ML Agent Response Validation

`GrpcMlAgentClient` validates every decoded `ChatEvent`: an unset `oneof` case (no
event type at all — the gRPC/protobuf analog of "unrecognized event name," since
protobuf's strong typing already rules out a malformed shape at the wire-format level)
becomes `MlAgentMalformedResponseException` (non-retryable, §9) instead of an
uncontrolled `NullPointerException` escaping the mapping stage. Two additional,
contract-specific invariants are enforced on every `payload` event before it becomes a
domain event (§2), via the shared
[`CaseSummaryPayloadValidator`](../src/main/java/com/cmbotservice/mlagent/CaseSummaryPayloadValidator.java):
every `keySignal` must carry at least one citation, and `suggestedResolution.mark` —
when present — must be one of the known resolution enum names
(`CONFIRMED_FRAUD | SUSPECTED_FRAUD | CONFIRMED_GENUINE | ASSUMED_GENUINE | UNKNOWN`;
`ANY` and the legacy single-character codes are both rejected as violations, not
accepted — see §2). Both violations also become `MlAgentMalformedResponseException` —
proven by dedicated in-process gRPC server tests, not just asserted.

## 17. Security Hardening (code-level; no gateway/infra here)

- `RequestContextResolver` isolates "who is calling" behind an interface —
  `HeaderBasedRequestContextResolver` (`X-User-Id` trust, `mode: NONE`) and
  `SessionRequestContextResolver` (validated BFF session, `mode: BFF_SESSION`, §20)
  are selected purely by `chatbot.security.mode`, with no controller/service change
  either way — this was the placeholder-to-real-authentication seam anticipated from
  the start, now actually exercised.
- No stack trace, gRPC/channel internal detail, or hostname ever reaches a response
  body — every error path funnels through `GlobalExceptionHandler` (pre-stream),
  `SessionAuthenticationWebFilter`'s own fixed rejection messages (§20), or the fixed
  `errorMessage` strings in `ChatOrchestrationServiceImpl#toErrorEvent` (in-stream).
- Correlation IDs are logged; auth-adjacent header/cookie values are not — the
  authentication flow logs only presence/length of the session cookie, never its
  value, and never the CSRF token or any access/refresh token (§20).
- Content-type/body validation is WebFlux's own default behavior (confirmed, not
  re-implemented).

## 18. Configuration Management

All tunables are typed, `@Validated` `@ConfigurationProperties` records
(`MlAgentProperties`, `ResilienceProperties`, `ChatProperties`) — a missing or invalid
mandatory value fails application startup, never surfaces as a mysterious runtime
failure on the first request. `ml-agent.mode: mock|grpc` selects the `MlAgentClient`
bean via `@ConditionalOnProperty` on each implementation — orchestration code depends
only on the interface, never a mode check.

## 19. Package Structure

```
com.cmbotservice
 ├─ config/     MlAgentProperties, ResilienceProperties, ChatProperties,
 │               GrpcChannelConfig, ResilienceConfig, OpenApiConfig
 ├─ context/    RequestContext, RequestContextResolver (interface),
 │               HeaderBasedRequestContextResolver (mode: NONE), RequestHeaders,
 │               CorrelationIdFilter (WebFilter), MdcContext
 ├─ security/   ChatbotSecurityMode, SecurityProperties, SessionContext,
 │               SessionStore (interface), JsonBlobSessionStore,
 │               SessionRequestContextResolver (mode: BFF_SESSION),
 │               SessionAuthenticationWebFilter, AuthRejectionReason,
 │               SecurityModeStartupLogger, CorsSecurityConfig, RedisSessionConfig
 ├─ web/
 │   ├─ controller/  ChatController
 │   ├─ dto/         ChatRequest, ErrorResponse
 │   └─ advice/      GlobalExceptionHandler
 ├─ service/    ChatOrchestrationService (interface), ChatOrchestrationServiceImpl,
 │               ChatMetrics
 ├─ mlagent/    MlAgentClient, MlAgentRequest, MlAgentStreamEvent, CaseSummaryPayload,
 │               CaseSummaryPayloadValidator, MockMlAgentClient, MockScenario,
 │               GrpcMlAgentClient, grpc.v1/ (generated: ChatAgentGrpc, ChatRequest,
 │               ChatEvent, Token, ToolCall, ToolResult, Payload, Done, Error)
 ├─ sse/        ChatSseEvent, SseEventType, SseEvents, StreamStartEvent,
 │               MessageChunkEvent, CaseSummaryEvent, StreamCompleteEvent, StreamErrorEvent
 └─ common/     ErrorCode, MlAgentException, MlAgentTimeoutException,
                 MlAgentUnavailableException, MlAgentCommunicationException,
                 MlAgentMalformedResponseException, MlAgentRejectedException,
                 MlAgentContinuationExpiredException, ConcurrencyLimitExceededException,
                 LogSanitizer
```

No `domain/` or `repository/` package — there is no entity to model and nothing to
persist. No standalone `InvalidChatRequestException` (present in some briefs'
illustrative exception list) — Bean Validation (`WebExchangeBindException` → 400)
already covers every concrete request-shape rule this service has; adding a second,
behaviorally-identical type would be exactly the unnecessary abstraction the brief
warned against.

## 20. Authentication (BFF Session)

Per a documented flow (two source images, not reproduced here) requiring this
backend to validate an existing BFF-issued session rather than authenticate
independently — read-only, against the same Redis instance FMC-PM-BFF uses.
`chatbot.security.mode` (`NONE` | `BFF_SESSION`) is the master toggle; everything
below only applies in `BFF_SESSION`. See README "Authentication" for the config
table and a curl example of both modes.

**Flow** (`SessionAuthenticationWebFilter`, ordered right after `CorrelationIdFilter`,
scoped to only `POST /api/v1/chat/messages` — actuator/Swagger stay reachable
unauthenticated, since a k8s prober has no session cookie):

1. Extract the session cookie (`chatbot.security.session.cookie-name`) → `401` if absent.
2. Look up the session in Redis via `SessionStore` (read-only) → `401` if not found,
   `503` (default) or fail-open (`chatbot.security.fail-open-on-redis-error`, local-dev
   only) if Redis itself is unreachable.
3. Parse into `SessionContext` (username, tenantId, permissions, organizations,
   access-token expiry, fingerprint).
4. Check expiry → `401` if passed. Fail-closed on a missing expiry too — treated as
   already-expired, not never-expiring, since every field but `fingerprint` is
   documented as always present.
5. Validate CSRF (`chatbot.security.csrf.enabled`) — cookie value must equal header
   value → `403` on any mismatch or absence.
6. Cross-check an optional tenant header against the session's tenant → `403` on
   mismatch; skipped entirely if the header isn't sent.
7. Filter the session's permissions to the `chatbot.security.authorization.required-permission-prefix`
   subset, expose as granted authorities → `403` if empty, unless
   `permit-when-no-chatbot-permissions` is set.
8. CORS restricted to `chatbot.security.cors.allowed-origins` via a standard Spring
   `CorsWebFilter`/`CorsConfiguration` (not hand-rolled), credentials allowed, no
   wildcard.
9. On success: `SessionContext` and the granted-authorities list are stored as
   exchange attributes; `SessionRequestContextResolver` (the `BFF_SESSION`
   `RequestContextResolver` implementation) reads `SessionContext` back out to
   populate `RequestContext.userId` — `ChatController` needed zero changes, exactly
   as `RequestContextResolver`'s own Javadoc anticipated (§17).

**Why rejections are written directly, not thrown**: a `WebFilter` runs upstream of
`DispatcherHandler`, so an exception thrown here never reaches
`@RestControllerAdvice` — it would fall through to Boot's generic default error
page instead, a different JSON shape than the `ErrorResponse` contract every other
error path already uses. `SessionAuthenticationWebFilter` builds and writes the
`ErrorResponse` body itself for exactly this reason.

**A real bug caught by its own tests**: the first version chained
`.flatMap(session -> continueWithSession(...)).switchIfEmpty(...)`. Since
`continueWithSession` returns `Mono<Void>`, which never emits a value even on
success, `switchIfEmpty` fired on *every* request, re-running a `SESSION_NOT_FOUND`
rejection after the response had already been committed — `UnsupportedOperationException`
on the second header-write attempt. Fixed by converting emptiness to a real,
distinguishable `Optional<SessionContext>` value before the `flatMap`, so
`switchIfEmpty`'s ambiguity never arises. Caught immediately by
`SessionAuthenticationWebFilterTest`, before it ever reached a running instance.

**A second bug caught only by live verification, not by any unit test**: the filter's
initial version had no path scoping at all, so it also rejected `/actuator/health`
with 401 — invisible to `SessionAuthenticationWebFilterTest` (which builds its own
exchanges directly) and to `ChatControllerAuthenticationTest` (which never happened
to call actuator endpoints), only surfacing when a live health-check poll timed out.
Fixed by exempting every path except `ApiPaths.CHAT_MESSAGES`.

**A genuine Spring Boot 4 platform quirk, unrelated to this feature's own logic**:
Boot 4.1.1's default Jackson autoconfiguration targets Jackson 3
(`tools.jackson.*`) and does not provide a classic
`com.fasterxml.jackson.databind.ObjectMapper` bean out of the box — nothing in this
app had depended on one via DI since the gRPC migration (protobuf doesn't touch
Jackson), so this was latent and invisible until this feature's constructor
injection surfaced it. Fixed with an explicit `ObjectMapper` `@Bean`
(`RedisSessionConfig`), configured to match Spring's own long-standing default
(ISO-8601 instants) so a filter-written `ErrorResponse` body is indistinguishable
from one `GlobalExceptionHandler` serializes. Registering a `ReactiveRedisConnectionFactory`
bean also silently pulled in a live-Redis-ping health contributor
(`DataRedisReactiveHealthContributorAutoConfiguration`) into `/actuator/health` —
excluded for the same reason the ML Agent circuit breaker is kept out of the
readiness group (§14): a downstream dependency's outage has its own explicit
handling here (fail-open config / a `503` response) and shouldn't also flip this
service's own health signal.

**The one thing genuinely unconfirmed** (flagged rather than assumed, per the
source design's own explicit callout to get this from the FMC-PM-BFF team before
finalizing): the exact Redis session key format and serialization.
`JsonBlobSessionStore` implements one reasonable default (a single JSON document per
session at a plain string key, field names fully configurable) behind the
`SessionStore` interface — selected via `chatbot.security.redis.strategy` — so a
different real layout needs a new implementation of that interface, not a rewrite of
the filter. Two smaller unconfirmed points, also flagged rather than guessed:
`chatbot.security.session.tenant-header-name` has no documented default (unlike the
cookie/CSRF names) — `X-Tenant-Id` is this service's own placeholder; and
"endpoint-level access enforced by required permission" is described as a general
mechanism with no concrete permission named for this service's one real endpoint, so
only the confirmed part (filter-and-reject-if-empty) is implemented.

## Verified end-to-end

Full test suite (79 tests: unit, `StepVerifier`/virtual-time, an in-process gRPC
server (the gRPC analog of MockWebServer), a mocked Redis template
(`JsonBlobSessionStoreTest`), and `RestTestClient` against a real random port —
including a dedicated `ChatControllerAuthenticationTest` for `mode: BFF_SESSION`
alongside the default-mode `ChatControllerTest`) green, including the ML Agent
contract's request field mapping over gRPC (now including `history`), all six event
types, both `payload` invariant validations, an unset-`oneof` malformed case, gRPC
`Status.Code` → exception mapping, in-stream `error` event code mapping,
`continuation`/`conversationId` round-tripping across two requests, `history` forwarded
untouched alongside them (and defaulting to an empty list, never `null`, when omitted),
and every authentication rejection path (§20) plus its success path and both
Redis-failure modes. Live curl
verification: success/slow/error/empty/rejected/continuation-expired scenarios;
blank/missing-field validation → 400; circuit breaker forced open via repeated
`trigger:error` → subsequent request rejected instantly with
`CONCURRENCY_LIMIT_REACHED`, mock never re-invoked, `/actuator/health/readiness`
stays UP throughout; mid-stream client disconnect → `SSE_CLIENT_CANCELLED` at INFO,
zero ERROR-level noise; `/actuator/health`, `/actuator/health/{liveness,readiness}`,
`/actuator/prometheus`, `/actuator/circuitbreakers` all reachable and correct; a
`grep -rn "\.block()\|Thread.sleep" src/main/java` sanity check returns zero hits in
production code; `tenantId`/`caseId`/`conversationId` MDC fields populating correctly
end-to-end after the `Hooks.enableAutomaticContextPropagation()` fix (§11) —
re-verified with a fresh app start and real curl calls, not just the unit tests. App
boot re-verified in both `ml-agent.mode: mock` (full chat flow works end-to-end) and
`ml-agent.mode: grpc` (channel/stub beans construct cleanly; with no real agent
running, a chat request correctly surfaces `ML_AGENT_UNAVAILABLE` rather than hanging
or crashing — proving the gRPC error-mapping path end-to-end even without a live
agent). Also re-verified live with `chatbot.security.mode: BFF_SESSION` and no real
Redis running: `/actuator/health` stays fast (~70ms) and unauthenticated, while
`POST /api/v1/chat/messages` without a session cookie correctly returns
`401 {"errorCode":"UNAUTHENTICATED",...}` in the same `ErrorResponse` shape used
everywhere else in this API.
