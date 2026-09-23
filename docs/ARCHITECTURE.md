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
   │  Headers: X-Tenant-Id, X-Org-Id (both required)
   │  Body: { caseId, history?, requestId?, endUserId?, message }
   ▼
ChatController
   → Bean Validation (WebExchangeBindException → 400, pre-stream — the only way a
   │   request fails before the SSE response commits at 200)
   → resolves RequestContext (tenant/organization from headers, case/user/correlationId)
   │   — missing/blank X-Tenant-Id or X-Org-Id → ResponseStatusException(400)
   │   → also pre-stream, same VALIDATION_ERROR shape as a bad body field
   → returns Flux<ServerSentEvent<Object>> — Spring subscribes and writes as elements arrive
   ▼
ChatOrchestrationService (fully reactive — no thread, no blocking call, anywhere here)
   → message-length check (runtime-configurable ceiling)
   → builds MlAgentRequest (history forwarded verbatim; there is no continuation/
   │   conversationId to echo — the contract has neither)
   → mlAgentClient.streamResponse(...)
       .transformDeferred(CircuitBreakerOperator)
       .transformDeferred(BulkheadOperator)
       .timeout(firstResponseTimeout, evt -> idleTimeout)
       .retryWhen(backoff, filtered to retryable-and-nothing-streamed-yet)
       .transform(withTotalDeadline)
   → relays each AnswerEvent verbatim → ServerSentEvent (event: oneof field name,
   │   data: canonical proto3 JSON, original field names — see SseEvents); no mapping
   → onErrorResume: a failure with no agent event of its own becomes a `service_error`
     event, not an HTTP status (the agent's own `error` event is just forwarded)
   ▼
MockMlAgentClient / GrpcMlAgentClient (impl of MlAgentClient)
   → streams the agent's own AnswerEvents (chunk/tool_call/tool_result/payload/done/
   │   error/ping) as received — no identifier is ever revealed; resending `history` on
   │   the next request is the caller's only resumption mechanism
   → (real client) never buffers the full response — a straight Flux, gRPC → Netty
```

**The finding that originally reshaped this design, now fully resolved by the
finalized contract:** an early integration pass assumed the ML Agent would assign a
conversation identifier and hand it back early in the stream. It doesn't — and
Thoughtful Labs' now-finalized contract confirms there is no such identifier
*anywhere*, not even on the terminal event: resumption is *only* ever "resend the
full `history` transcript," full stop. Nothing in `done` (or any other event) carries
anything to echo. This finding predates
and is independent of the HTTP→gRPC transport swap — it's a property of the ML
Agent's own contract, not of either transport.

### The real ML Agent request/response contract

Thoughtful Labs' finalized 3-file protobuf contract (`src/main/proto/common.proto`,
`case_manager.proto`, `chat_agent.proto` — see §6 for the live schema), carried over
gRPC's server-streaming `ChatAgent.AskCaseManager` RPC. This superseded an earlier,
single-file contract (`Chat`/`ChatRequest`/`ChatEvent`, originally itself a translation
of a documented `POST /v1/chat` + SSE contract) — this section describes the current,
finalized shape only; shown here as JSON for readability, field names map 1:1 to the
`.proto` message fields (`camelCase` JSON ↔ `snake_case` proto).

Request (`AskCaseManagerRequest`):

```json
{
  "requestContext": {
    "tenant": "shcuat1b",
    "organization": "org-7",
    "agentSessionId": "req-a1b2c3",
    "requestId": "<this backend's own correlation id>"
  },
  "operatorId": "analyst-1",
  "prompt": "Summarise this case",
  "caseContext": { "caseId": "1234", "endUserId": null },
  "history": [
    { "user": { "prompt": "Summarise this case" } },
    { "agent": { "text": "..." } }
  ]
}
```

**There is no continuation token or conversation id anywhere in this contract** —
`history` (a `ConversationTurn` `user`/`agent` oneof per turn) is the *only*
resumption mechanism, and it is the caller's sole responsibility: this service
forwards it exactly as supplied (`ChatRequest` → `MlAgentRequest` → the proto
`ConversationTurn`), never assembling, storing, or interpreting it. This backend's
own `HistoryTurn` (`role`/`content`) is a stable internal/DTO shape, not the wire
format — `GrpcMlAgentClient#toConversationTurn` translates `role == "user"` into a
user turn and anything else into an agent turn.

`requestContext.organization` is sourced from the required `X-Org-Id` request
header (`RequestContextResolver` → `RequestContext.organization` → `MlAgentRequest
.organization`), forwarded to the ML Agent as-is — this backend does not look it up,
validate it against `SessionContext.organizations` (a list, in `BFF_SESSION` mode), or
otherwise interpret it. `requestContext
.agentSessionId` reuses the caller's optional `requestId` (a loose grouping hint, per
the contract's own "groups related requests for tracing and metrics" — it carries no
server-side state); `requestContext.requestId` is this backend's *own* correlation id
(from `X-Correlation-Id`/generated), not the caller's `requestId` field, despite the
similarly-named fields on both sides. `operatorId` is sourced from the existing
analyst identity (`RequestContext.userId()`). The product/prod-auth path
(`context_token` on the old contract) has no equivalent field at all in this finalized
contract — moot, not merely unwired.

Seven event types (`AnswerEvent`), an unset or unrecognised `event` oneof arm MUST be
(and is) silently ignored per the contract, never treated as an error:

| Event | Payload | Cardinality |
|---|---|---|
| `chunk` | `{ delta }` | many — streaming answer text |
| `tool_call` | `{ tool_call_id, name, args_json }` | 0..n |
| `tool_result` | `{ tool_call_id, status, ms, row_count? }` | one per `tool_call` |
| `payload` | structured Case Manager analysis (below), wrapped in a module-union `AnswerPayload` oneof | at most one, before `done` |
| `done` | `{ stop_reason, latency_ms, tokens_in, tokens_out }` | one, terminal |
| `error` | `{ code, retryable }` | terminal |
| `ping` | (empty) | 0..n — pure transport keepalive |

Every one of these is forwarded to the frontend unchanged (§5).

`payload` (`CaseManagerAnswerPayload`, the Case Manager module's payload arm):

```json
{
  "key_signals": [{ "signal": "...", "citations": ["7ff7-..._TRX"] }],
  "citations": [{ "id": "7ff7-..._TRX", "source": "AgenticGetCase.events[].decision", "fields": ["ri..."] }]
}
```

This is deliberately much smaller than the earlier contract's `payload` — no
top-level `answer`, no `summary.narrative`/`entities`/`timeline`, no
`suggestedResolution` at all. It's an overlay of structured signals/citations on top
of the answer text (which arrives via `chunk` events), not a duplicate of the answer
itself. The contract's one invariant — every `key_signal` carries ≥1 citation ("a
signal without a resolvable citation is a defect, not a soft failure") — is now only
*observed* by `GrpcMlAgentClient` (a WARN log, §16), not enforced: the payload is
forwarded as-is either way. The whole `suggestedResolution`/
resolution-mark validation this doc previously described no longer applies — there is
no such field to validate.

`done`'s `stop_reason` (`STOP_REASON_COMPLETED` / `STOP_REASON_TRUNCATED`) reaches the
frontend as the agent's own enum value; this backend no longer collapses it into a
boolean of its own.

Errors — a 3-value `Error.Code` enum plus an explicit `retryable` boolean, used either
as the gRPC status of the initial `AskCaseManager` call (pre-stream — mapped from the
closest-matching `Status.Code`, since gRPC has no literal "401") or as the terminal
`error` event's `code`/`retryable` (in-stream):

| Code | Meaning | Retryable |
|---|---|---|
| `ERROR_CODE_MODEL_REFUSED` | The agent refused to process the request | never (per contract) |
| `ERROR_CODE_DATA_UNAVAILABLE` | The agent's data source was unavailable | per the `retryable` field |
| `ERROR_CODE_INTERNAL` | Internal agent error | per the `retryable` field |
| `ERROR_CODE_UNSPECIFIED` / unrecognised | Treated as `ERROR_CODE_INTERNAL` per the contract | per the `retryable` field |

This is a real simplification from the old contract's ten string codes
(401/403/404/422/429/503/4221/4222). In-stream, the `error` event is **forwarded to the
frontend as-is** — `code` and `retryable` exactly as the agent sent them — rather than
converted into an exception and re-expressed as a backend `errorCode`, so the old
`ErrorCode.ML_AGENT_REFUSED` translation no longer exists. `ChatOrchestrationServiceImpl`
only observes it (an `ML_AGENT_ERROR_EVENT` WARN log, and the stream counted as
`ml.requests.total{result=failed}`). Pre-stream, the gRPC `Status.Code` is still mapped to
`MlAgentRejectedException`/`MlAgentCommunicationException`/`MlAgentUnavailableException`,
since a status has no agent event to forward; those surface as `service_error` (§5).

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

One endpoint: `POST /api/v1/chat/messages`. `tenantId`/`organization` travel as the
required `X-Tenant-Id`/`X-Org-Id` request headers; `caseId`/`history`/
`requestId`/`endUserId` travel in the request body — there is no backend-owned
resource to nest a path segment under. `history` is optional (omit or send empty to
start a new conversation, resend the growing transcript to continue one); this backend
never stores, assembles, or interprets it.

The one architectural line that matters here: **Bean Validation and header presence
checks are the only things that can produce a non-200 HTTP status.** A missing/blank
`X-Tenant-Id`/`X-Org-Id` is rejected via `ResponseStatusException(400)` from
`RequestContextResolver`, mapped by `GlobalExceptionHandler` to the same
`VALIDATION_ERROR` shape as a Bean Validation failure. Once `ChatOrchestrationService`
returns its `Flux`, Spring commits the response at 200/`text/event-stream` as soon as
it starts writing — there is no way to change the status after that. So every failure that can
occur once the ML Agent call has actually begun (timeout, circuit-breaker-open,
bulkhead-full, retry-exhausted, transport failure) is deliberately funneled through
one `.onErrorResume(...)` into a `service_error` SSE event instead, on the same stream.
The ML Agent's own model-level `error` event never takes this path — it is an ordinary
stream element, forwarded as-is. This is documented explicitly on the
`@Operation` Swagger annotation so a frontend integrator doesn't go looking for a 503.

## 5. SSE Event Contract

**The ML Agent's response is forwarded to the frontend as-is — this backend is a
transparent proxy for the response contract.** There is no backend-owned response
schema: no renamed events, no renamed fields, no reshaped payloads, no wrapper. An
earlier revision translated every event into its own DTOs (`stream-start`, `message`,
`tool-call`, `tool-result`, `payload`, `stream-complete`, with camelCase fields plus
`messageId`/`timestamp`/`sequence`/`totalChunks`); that translation layer was removed
so the frontend consumes Thoughtful Labs' contract directly, and a change to the
agent's response messages needs only a `.proto` update here, no Java mapping.

`Content-Type: text/event-stream`. For each `AnswerEvent`
([`SseEvents`](../src/main/java/com/cmbotservice/sse/SseEvents.java)):

| SSE field | Value |
|---|---|
| `event:` | the name of the `AnswerEvent` oneof field that is set — `chunk`, `tool_call`, `tool_result`, `payload`, `done`, `error`, `ping` — read from the protobuf descriptor (`findFieldByNumber(eventCase.getNumber()).getName()`), never a hand-written mapping |
| `data:` | the whole `AnswerEvent`, rendered by `JsonFormat.printer().preservingProtoFieldNames().alwaysPrintFieldsWithNoPresence().omittingInsignificantWhitespace()`: original `.proto` field names and nesting (`{"payload":{"case_manager_answer_payload":{"key_signals":[...]}}}`), enums as proto names, default-valued fields still printed (oneof members such as `row_count` only when set). Per the proto3 JSON mapping, `int64` fields are JSON strings. Handed to Spring as a pre-rendered `String`, which its SSE writer emits without a second JSON encoding |
| `id:` | a 0-based frame counter, assigned once via `Flux#index()` — SSE transport metadata, the only thing this backend adds to an agent event |

The canonical proto3 JSON mapping was chosen over a hand-rolled serializer because it
is lossless (the `data:` round-trips through `JsonFormat.parser()` to an identical
message — pinned by `SseEventsTest`) and is what any other protobuf consumer would
produce for the same message.

Forwarded: every event type, including `ping` (keeps intermediaries from idling the
SSE connection, and resets the idle timeout, §8) and the agent's `error`. Withheld: only
an `AnswerEvent` with its `event` oneof unset — per the contract, clients MUST ignore it,
and there is nothing recognisable to forward (unknown fields are not representable in
proto3 JSON without the schema).

**The one backend-defined event: `service_error`**
([`ServiceErrorEvent`](../src/main/java/com/cmbotservice/sse/ServiceErrorEvent.java)) —
`{ messageId, errorCode, errorMessage, timestamp }`, terminal — exists only because some
failures produce no agent event at all, after the response has already committed at 200:
the agent could not be reached, timed out, rejected the call with a gRPC status, or this
backend's circuit breaker/bulkhead/message-length check stopped the request. It
deliberately uses a name the agent never uses, so a frontend never confuses it with the
agent's `error` (a different payload). `errorCode` is one of `ML_AGENT_TIMEOUT`,
`ML_AGENT_UNAVAILABLE`, `ML_AGENT_ERROR`, `CONCURRENCY_LIMIT_REACHED` (bulkhead full
*or* circuit open), `NOT_FOUND`/`VALIDATION_ERROR`/`INTERNAL_ERROR`. `errorMessage` is
always a fixed, generic, client-safe string — never an exception message, stack trace,
or gRPC/hostname detail.

Every stream ends with exactly one of `done`, `error`, or `service_error`.

## 6. `MlAgentClient` — the reactive contract

```java
public interface MlAgentClient {
    Flux<AnswerEvent> streamResponse(MlAgentRequest request);
}
```

The element type is the ML Agent's own generated `AnswerEvent` — there is no
backend-owned response domain model (the former sealed `MlAgentStreamEvent` and
`CaseSummaryPayload` existed only to rename/reshape it and were removed). Transport
failures still flow through the `Flux`'s native error channel as `MlAgentException`
subtypes, which is what lets `.timeout()`, `.retryWhen()`, and the resilience4j
operators compose declaratively in `ChatOrchestrationService`; the agent's model-level
`error` is an ordinary element.

**`MockMlAgentClient`** — no threads, no polling loops, nothing that could leak:
emits the real contract's messages — `Flux.concat(toolTrace, chunks.delayElements(delay),
Mono.just(payload), Mono.just(done))` for success/slow (a `tool_call`/`tool_result`
pair, `chunk`s, a `payload` with a citation on every key signal, then `done`), so mock
mode puts the exact production wire format on the stream; `Flux.error(...)` for
`trigger:error`/`trigger:rejected` (a transport failure, as a gRPC status would be);
a single agent `error` event for `trigger:agent-error`; `Flux.just(done)` for empty;
`Flux.never()` for timeout — the orchestrator's own timeout operator is what ends
that one, not the mock. `delayElements` natively respects cancellation, so a
disconnect mid-slow-stream stops everything downstream for free. Entirely
transport-agnostic — this class never changed across either transport swap
(placeholder → HTTP → gRPC), and needed no `conversationId`/`continuation`
echo-or-fabricate logic once the finalized contract confirmed neither exists.

**`GrpcMlAgentClient`** — built from the shared `ManagedChannel`/`ChatAgentStub` (§7),
calls the ML Agent's `AskCaseManager` server-streaming RPC
(`src/main/proto/{common,case_manager,chat_agent}.proto`). grpc-java's generated
async stub is callback-based (`StreamObserver`), not `Flux`-based, so
`grpcEventFlux` bridges the two manually via `Flux.create` plus grpc-java's own manual
flow-control API (`ClientCallStreamObserver#disableAutoRequestWithInitial(0)`/
`request(n)`) — giving real backpressure without pulling in a third-party
reactive-grpc codegen plugin. One sharp edge worth documenting explicitly since it
cost real debugging time: `request()`/`cancel()` **cannot** be called synchronously
inside `ClientResponseObserver#beforeStart(...)` — grpc-java throws
`IllegalStateException: Not started`, because `beforeStart` runs *before* the
underlying `ClientCall.start()`. The fix is to stash the `ClientCallStreamObserver`
reference in `beforeStart` and only wire `sink.onRequest(...)`/`sink.onCancel(...)` to
it *after* the `stub.askCaseManager(...)` call returns (which is exactly when
`start()` has finished) — see `GrpcMlAgentClient#grpcEventFlux`.

A second sharp edge, found in `mode: grpc` against the real agent: the convenience
`disableAutoInboundFlowControl()` is `disableAutoRequestWithInitial(1)`, so gRPC
auto-requests one message on top of every `request(n)` forwarded from Reactor. The agent
can then deliver one event more than downstream demand, which `Flux.create`'s
`OverflowStrategy.ERROR` rejects with `OverflowException`. That only happens when demand
is bounded (the SSE writer's is) and the first event arrives after the demand wiring (a
real network, not the in-process `directExecutor` test server). The fix is an initial
request of `0`, so gRPC delivers exactly what Reactor asks for. It's pinned by
`GrpcMlAgentClientTest#boundedDownstreamDemand_...`, which sends from a separate server
thread specifically to reproduce that timing.

Never buffers the full response (no `collectList()`/`.block()` anywhere — proven by
the in-process gRPC tests, not just claimed), and emits each `AnswerEvent` it
receives unchanged — the tests assert deep `equals` between what the in-process server
sent and what the client emitted. The only per-event work is observation (DEBUG/TRACE
logs of tool calls/results/pings, a WARN on a `payload` violating the citation
invariant, §16) plus filtering out unset-oneof events. The initial call's gRPC
`Status.Code` is still translated into `MlAgentRejectedException`/
`MlAgentCommunicationException`/`MlAgentUnavailableException`, since a status is not an
agent event and has nothing to forward.

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

**Build-time codegen**: `src/main/proto/{common,case_manager,chat_agent}.proto` —
Thoughtful Labs' finalized 3-file contract, kept flat under `src/main/proto/` (no
subdirectory) consistent with this repo's existing convention, all three sharing one
package (`cmbotservice.mlagent.v1` / `com.cmbotservice.mlagent.grpc.v1`) — is compiled
into `ChatAgentGrpc`/message classes by `protobuf-maven-plugin` (+ the `os-maven-plugin`
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
    .doOnNext(evt -> { firstEventSeen.set(true); ... })  // must run before retryWhen
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
something it isn't shaped for. "First event" means the first real `AnswerEvent` from the
agent: both clients used to emit a synthetic `Started` marker before the gRPC call had
even returned anything, which set this flag immediately and meant a pre-first-event
failure was in practice never retried. With that marker removed (it only existed to
produce the backend's own `stream-start` event), the retry now engages exactly as
designed. One consequence: every failed attempt counts toward the circuit breaker, so
a run of unreachable-agent failures opens it faster than before.

`isRetryable(Throwable)`: `MlAgentUnavailableException` (connection-level) and
`MlAgentCommunicationException` (a transient gRPC status such as `RESOURCE_EXHAUSTED`/
`INTERNAL`) → retryable. The agent's own in-stream `error` event is never retried by
this backend — it is forwarded, and its `retryable` flag is the frontend's to act on. `MlAgentMalformedResponseException`, `MlAgentTimeoutException`,
`MlAgentRejectedException` (a pre-stream rejection status), `BulkheadFullException`,
`CallNotPermittedException` → not retryable. Timeout is
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
`trigger:error`, the very next request gets an immediate `service_error` event
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
`chain.filter(exchange)`. `tenantId`/`caseId` are added to the same `Context` once the
request body is parsed, at the top of `ChatOrchestrationService.streamMessage`.

Getting those `Context` values to actually show up in MDC on whatever thread happens
to be running at log time — without an unsafe shared `ThreadLocal` — is exactly what
Micrometer's `ContextRegistry`/`ThreadLocalAccessor` SPI exists for.
[`MdcContext`](../src/main/java/com/cmbotservice/context/MdcContext.java) registers
three `ThreadLocalAccessor<String>` instances at startup (`correlationId`/`tenantId`/
`caseId`), and Reactor restores each one into MDC around every operator boundary,
scoped correctly per-subscription — *provided* automatic context propagation is
actually switched on.

**That last part was a real bug, not an assumption that happened to hold.** The
original version of this section claimed Spring Boot enables
`Hooks.enableAutomaticContextPropagation()` automatically once `context-propagation`
is on the classpath (pulled in transitively by `micrometer-tracing-bridge-otel`).
That turned out not to be happening: every registered key — `tenantId`, `caseId` —
showed up blank in every log line across the entire reactive migration, and it went
unnoticed because verification focused on SSE content, actuator endpoints, and
cancellation/circuit-breaker behavior rather than scrutinizing the MDC fields in the
log output themselves. It surfaced only once the log output was specifically checked
end-to-end. Fixed by calling `Hooks.enableAutomaticContextPropagation()` explicitly in
`MdcContext`'s `@PostConstruct`, right next to the accessor registration, so the two
can't drift out of sync again — verified live afterward: `tenantId`/`caseId` (and
`correlationId`, which worked before since it's also readable directly off the
exchange, independent of this mechanism) all populate correctly.

One related gap remains, **not** fixed by the above, called out explicitly rather than
implied fixed (see README "Known limitations"): `traceId`/`spanId` still show blank
despite `http.server.requests` metrics confirming request observations are genuinely
being created — so Micrometer Tracing's own MDC bridging has a separate, still-open
problem, not yet root-caused. The earlier, related MDC gap this doc used to describe
(a `conversationId` key that never got backfilled mid-stream) no longer applies —
the finalized ML Agent contract has no conversation identifier of any kind to
backfill.

**No separate client-owned "chat window" tracing token exists in this contract.** An
earlier iteration added one (`X-Window-Id`, a client-generated ID meant to stay stable
across a conversation reset) — it was removed as unnecessary: `correlationId` already
identifies one request, and the finalized contract has no per-conversation identifier
for a second token to shadow. Log/monitoring correlation for one chatbot interaction is
fully covered by `correlationId`/`tenantId`/`caseId` alone.

This is also the tenant-safety story: `RequestContext` is passed as an explicit
parameter through every service method (never ambient/thread-local), and Reactor
`Context` is inherently per-subscription — there is no shared mutable state for one
request's tenant/case/conversation identifiers to leak into another's, even under
concurrency, even by accident.

## 12. Logging

SLF4J + Logback, color-coded console pattern (kept from the earlier iteration — see
README). MDC keys `correlationId`/`tenantId`/`caseId`/`traceId`/`spanId` appear on
every log line for one chatbot interaction (§11). Logical event
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
to accidentally tag a metric with `caseId`/`userId` (the brief's own
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
breaker, `service_error` events, and metrics — not by removing an otherwise-healthy instance
from rotation. The indicator still contributes to the plain `/actuator/health`
aggregate, which is useful signal there.

`server.shutdown: graceful` + `spring.lifecycle.timeout-per-shutdown-phase` — Reactor
Netty's graceful shutdown (stop accepting new connections, let in-flight
requests/streams finish up to the timeout, then close) is built into Boot for this
server type; pure configuration, no custom lifecycle code.

## 15. Validation, Sizing, DTO Boundaries

`ChatRequest` (frontend-facing): `@NotBlank`/`@Size`/`@Pattern` on `caseId`
(identifier charset + length hardening; `tenantId`/`organization` are validated
separately, as required headers, by `RequestContextResolver`), `@Size(max=4000)` hard
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

DTO boundary: the **request** side is still mapped — `ChatRequest` (frontend) →
`MlAgentRequest` (built explicitly by `ChatOrchestrationServiceImpl`) →
`AskCaseManagerRequest` (the proto, built by `GrpcMlAgentClient`). The **response** side
deliberately is not: the agent's `AnswerEvent` goes straight to a `ServerSentEvent`,
so the frontend consumes the ML Agent's response contract directly and its changes
propagate to the frontend by design (§5).

## 16. ML Agent Response Validation

This backend no longer judges the content of the agent's events; it forwards them.
An unset (or unrecognised) `event` oneof case is ignored per the contract ("clients
MUST ignore events whose `event` oneof is unset or unrecognised and continue reading
the stream" — logged at DEBUG, stream continues). The contract's `payload` invariant
(every `key_signal` carries ≥1 citation) and a `payload` with no recognised arm are
reported as WARN logs by `GrpcMlAgentClient` but the event is still forwarded as-is —
rejecting the whole answer would be this backend overriding the agent's response, and
the contract already requires clients to tolerate unresolvable citation ids. (This
used to fail the stream via `CaseSummaryPayloadValidator`, since removed.)
`MlAgentMalformedResponseException` now covers only an event that cannot be rendered
as JSON at all.

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
  authentication flow logs only presence/length of the session cookie, never its value
  or the session's `access_token`/`refresh_token` (§20). **Deliberate, temporary
  exception**: `SessionDebugController` (§20) returns `accessToken`/`refreshToken` in
  its HTTP response body by design — that's its entire purpose (verifying the Redis
  lookup) — but still never logs them; remove that endpoint before this service is
  exposed anywhere those response bodies could be captured/cached.
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
 │   ├─ controller/  ChatController, SessionDebugController (temporary, mode: BFF_SESSION)
 │   ├─ dto/         ChatRequest, ErrorResponse, SessionDebugResponse
 │   └─ advice/      GlobalExceptionHandler
 ├─ service/    ChatOrchestrationService (interface), ChatOrchestrationServiceImpl,
 │               ChatMetrics
 ├─ mlagent/    MlAgentClient, MlAgentRequest, MockMlAgentClient, MockScenario,
 │               GrpcMlAgentClient, grpc.v1/ (generated: ChatAgentGrpc,
 │               AskCaseManagerRequest, AnswerEvent, AnswerPayload,
 │               CaseManagerAnswerPayload, AgentRequestContext, ConversationTurn,
 │               Chunk, ToolCall, ToolResult, Done, Error, Ping)
 ├─ sse/        SseEvents (AnswerEvent → SSE, verbatim), SseEventType,
 │               ServiceErrorEvent
 └─ common/     ErrorCode, MlAgentException, MlAgentTimeoutException,
                 MlAgentUnavailableException, MlAgentCommunicationException,
                 MlAgentMalformedResponseException, MlAgentRejectedException,
                 ConcurrencyLimitExceededException, LogSanitizer
```

No `domain/` or `repository/` package — there is no entity to model and nothing to
persist. No standalone `InvalidChatRequestException` (present in some briefs'
illustrative exception list) — Bean Validation (`WebExchangeBindException` → 400)
already covers every concrete request-shape rule this service has; adding a second,
behaviorally-identical type would be exactly the unnecessary abstraction the brief
warned against.

## 20. Authentication (BFF Session)

Per a documented flow (`docs/chatbot_auth_redis_lookup.png`) requiring this backend to
validate an existing BFF-issued session rather than authenticate independently —
read-only, against the same Redis instance FMC-PM-BFF uses. `chatbot.security.mode`
(`NONE` | `BFF_SESSION`) is the master toggle; everything below only applies in
`BFF_SESSION`. See README "Authentication" for the config table and a curl example of
both modes.

**Current scope is deliberately minimal, by explicit request, and narrower than the
diagram**: `sessionId + tenant → Redis lookup → session found → extract accessToken →
authenticated → proceed`. A record found at `session:<sessionId>:<tenant>` at all is
trusted as authenticated — there is currently **no** fingerprint, CSRF, permission, or
token-expiry validation, even though the diagram calls for all four and an earlier
iteration of this feature implemented them. That richer logic was deliberately removed
rather than left dormant/half-wired — see the git history for
`SessionAuthenticationWebFilter`/`JsonBlobSessionStore` if it needs to come back, and
reintroduce it as a deliberate scope decision rather than guessing at partial
reinstatement.

**Flow** (`SessionAuthenticationWebFilter`, ordered right after `CorrelationIdFilter`,
scoped to only `POST /api/v1/chat/messages` — actuator/Swagger stay reachable
unauthenticated, since a k8s prober has no session cookie):

1. Extract the session cookie (`chatbot.security.session.cookie-name`) → `401` if
   absent. Extract the tenant from the `chatbot.security.session.tenant-header-name`
   header (default `X-Tenant-Id` — the same always-required header, reused rather than
   a separate custom one) → `401` if absent — it's part of the Redis key itself.
2. Look up `session:<sessionId>:<tenant>` in Redis via `SessionStore` (read-only) →
   `401` if not found, `503` (default) or fail-open
   (`chatbot.security.fail-open-on-redis-error`, local-dev only) if Redis itself is
   unreachable. The stored value is `{context_json, access_token, refresh_token, fp}`;
   `context_json` is itself a JSON *string*, parsed a second time for
   `username`/`tenantId`. A blank `context_json`/`access_token` or missing `username`
   is treated the same as "not found" (`JsonBlobSessionStore`) — this is basic parsing
   correctness, not an authentication check.
3. **Found at all ⇒ authenticated.** `SessionContext` (username, tenantId,
   accessToken, refreshToken, contextJson, fingerprint — every envelope field carried
   through unvalidated) is stored as an exchange attribute; `SessionRequestContextResolver`
   (the `BFF_SESSION` `RequestContextResolver` implementation) reads it back out to
   populate `RequestContext.userId`/`RequestContext.accessToken` — `ChatController`
   needed zero changes, exactly as `RequestContextResolver`'s own Javadoc anticipated
   (§17). `accessToken` is not yet forwarded anywhere — a future task wires it into the
   call to the TFLabs Orchestrator Service.
4. CORS restricted to `chatbot.security.cors.allowed-origins` via a standard Spring
   `CorsWebFilter`/`CorsConfiguration` (not hand-rolled), credentials allowed, no
   wildcard. Unaffected by the scope reduction above — orthogonal concern.

**Temporary verification endpoint**: `GET /api/v1/debug/session-lookup`
(`SessionDebugController`, `docs/ARCHITECTURE.md` §19's `web/controller/` list) reuses
the exact same `SessionStore.findSession(...)` call — deliberately, so there is exactly
one Redis lookup implementation — to let the shared Redis lookup be verified directly
from Swagger UI (session cookie as a `COOKIE`-in parameter, tenant as a header) without
exercising the full chat flow. Returns the full raw envelope
(`accessToken`/`refreshToken`/`contextJson`/`fingerprint`) on success, `401` if no
record exists, `503` if Redis is unreachable. Only registered in `mode: BFF_SESSION`
(same `@ConditionalOnProperty` `SessionStore` beans use). **Not part of this service's
stable API** — it exists only to make the current, minimal Redis-lookup flow easy to
manually verify, and should be removed once that's no longer needed.

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
from one `GlobalExceptionHandler` serializes.

**A second platform quirk, found only once Azure Entra ID passwordless auth was
wired in**: the Lettuce-based `RedisConnectionFactory` Boot autoconfigures actually
implements *both* the blocking and reactive connection-factory interfaces — the
`ReactiveRedisConnectionFactory` bean this feature depends on
(`RedisSessionConfig#sessionRedisTemplate`) comes from `DataRedisAutoConfiguration`
(the "blocking" one), not `DataRedisReactiveAutoConfiguration` (which only adds
`ReactiveRedisTemplate`/`ReactiveStringRedisTemplate` *on top of* an already-existing
factory bean, via its own `@ConditionalOnBean`). Excluding `DataRedisAutoConfiguration`
— which an earlier version of this config did, on the assumption "blocking, therefore
unneeded in a fully-reactive app" — silently left no connection factory bean at all.
Only `DataRedisReactiveHealthContributorAutoConfiguration`/
`DataRedisHealthContributorAutoConfiguration` are excluded now, for the same reason
the ML Agent circuit breaker is kept out of the readiness group (§14): a downstream
dependency's outage has its own explicit handling here (fail-open config / a `503`
response) and shouldn't also flip this service's own health signal.

**The Redis session key format and session envelope are now confirmed** (see
`docs/chatbot_auth_redis_lookup.png`), resolving what an earlier iteration of this
feature had flagged as genuinely unconfirmed: one JSON document per session at key
`session:<sessionId>:<tenant>`, `{context_json, access_token, refresh_token, fp}` with
`context_json` itself a JSON string. `JsonBlobSessionStore` implements exactly this
behind the `SessionStore` interface (selected via `chatbot.security.redis.strategy`) —
a structurally different real layout would still need a new implementation of that
interface, not a rewrite of the filter. The diagram's separate CSRF-token key
(`csrf:<sessionId>:<tenant>`), fingerprint check, `CHATBOT_`-permission filter, and
JWT-expiry check are all real, previously-implemented pieces of that richer flow — not
implemented *today* per the current scope decision (see above), not unconfirmed or
forgotten.

## Verified end-to-end

Full test suite (82 tests: unit, `StepVerifier`/virtual-time, an in-process gRPC
server (the gRPC analog of MockWebServer), a mocked Redis template
(`JsonBlobSessionStoreTest`), and `RestTestClient` against a real random port —
including a dedicated `ChatControllerAuthenticationTest` for `mode: BFF_SESSION`
alongside the default-mode `ChatControllerTest`) green, including the ML Agent
contract's request field mapping over gRPC (`history` translated into the `user`/
`agent` oneof), all seven event types forwarded as the identical proto message and
rendered as SSE with the agent's own event names and field names (`SseEventsTest`
round-trips every one back through `JsonFormat.parser()`), the agent's `error` event
and a citation-less `payload` forwarded untouched, an unset-`oneof` event ignored
per the finalized contract, gRPC `Status.Code` → exception → `service_error` mapping,
and `history` forwarded untouched (defaulting to an empty list, never `null`,
when omitted), and every authentication rejection path (§20) plus its success path
and both Redis-failure modes. Live curl verification: success/slow/error/empty/
rejected scenarios; blank/missing-field validation → 400 (including a missing
`X-Tenant-Id` or `X-Org-Id` header); circuit breaker forced open
via repeated `trigger:error` → subsequent request rejected instantly with
`CONCURRENCY_LIMIT_REACHED`, mock never re-invoked, `/actuator/health/readiness`
stays UP throughout; mid-stream client disconnect → `SSE_CLIENT_CANCELLED` at INFO,
zero ERROR-level noise; `/actuator/health`, `/actuator/health/{liveness,readiness}`,
`/actuator/prometheus`, `/actuator/circuitbreakers` all reachable and correct; a
`grep -rn "\.block()\|Thread.sleep" src/main/java` sanity check returns zero hits in
production code; `tenantId`/`caseId` MDC fields populating correctly end-to-end after
the `Hooks.enableAutomaticContextPropagation()` fix (§11) — re-verified with a fresh
app start and real curl calls, not just the unit tests. App
boot re-verified in both `ml-agent.mode: mock` (full chat flow works end-to-end) and
`ml-agent.mode: grpc` (channel/stub beans construct cleanly; with no real agent
running, a chat request correctly surfaces `ML_AGENT_UNAVAILABLE` rather than hanging
or crashing — proving the gRPC error-mapping path end-to-end even without a live
agent). Also re-verified live with `chatbot.security.mode: BFF_SESSION` and no real
Redis running: `/actuator/health` stays fast (~70ms) and unauthenticated, while
`POST /api/v1/chat/messages` without a session cookie correctly returns
`401 {"errorCode":"UNAUTHENTICATED",...}` in the same `ErrorResponse` shape used
everywhere else in this API.
