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

Since the real ML Agent API isn't available yet, all ML functionality is behind
[`MlAgentClient`](../src/main/java/com/cmbotservice/mlagent/MlAgentClient.java), with
two implementations selected purely by configuration
(`@ConditionalOnProperty(ml-agent.mode)`, never a runtime `if/else`):
[`MockMlAgentClient`](../src/main/java/com/cmbotservice/mlagent/MockMlAgentClient.java)
(default) and
[`HttpMlAgentClient`](../src/main/java/com/cmbotservice/mlagent/HttpMlAgentClient.java)
(real, WebClient-based).

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
MockMlAgentClient / HttpMlAgentClient (impl of MlAgentClient)
   → streams Started → Token* → Payload → Done — conversationId/continuation are
     revealed only on Done, never before (see the callout below)
   → (real client) never buffers the full response — a straight Flux, WebClient → Netty
```

**The one finding that reshaped this design:** the real ML Agent (`POST /v1/chat`,
built by Thoughtful Labs) never reveals a conversation identifier until its final
`done` event — there is no early "here's your conversation id" moment. An earlier
placeholder version of this contract assumed the agent assigned one up front and
handed it back on `Started`; it doesn't, so `MlAgentStreamEvent.Started` carries no
fields at all, and `conversationId`/`continuation` only become authoritative on
`MlAgentStreamEvent.Done` → `StreamCompleteEvent`. Every SSE event before
`stream-complete` echoes whatever the *request* supplied (or blank for a new
conversation) — documented explicitly on `StreamStartEvent` as "not authoritative."

### The real ML Agent request/response contract

Request body (`POST /v1/chat`):

```json
{
  "continuation": "v1.k3.eyJlbmMi...",
  "conversationId": null,
  "surface": "case_manager",
  "message": "Summarise this case",
  "context": { "caseId": "1234", "endUserId": "gadi5" },
  "tenantId": "shcuat1b",
  "options": { "includeResolutions": true }
}
```

`history` (an explicit transcript, the contract's third resumption mechanism) and
`contextToken` (the product/prod-auth path) are deliberately never sent —
`history` is "eval only" and would require storing/replaying messages, directly
against this service's stateless design; `contextToken` has no real auth/JWT
infrastructure behind it yet (documented gap, not a bug). Per the contract's own
guidance, this service never chooses between `continuation`/`conversationId` — it
just echoes back whatever the previous `done` event returned, verbatim.

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
  "suggestedResolution": { "mark": "S", "label": "Suspected Fraud", "confidence": "medium", "rationale": "..." },
  "citations": [{ "id": "7ff7-..._TRX", "source": "APP_EVENT_LOG", "fields": ["risk_score"] }]
}
```

Two invariants `HttpMlAgentClient` enforces rather than trusts blindly (§16): every
`keySignal` must carry ≥1 citation ("a signal without a resolvable citation is a
defect, not a soft failure"), and `suggestedResolution.mark` is always one of
`F S G A U Y B T X C` ("the agent never invents a label" — the full set is validated
permissively since a tenant flag this backend can't see controls whether `X`/`C` are
in play).

Errors — one unified code space, used either as the HTTP status of the initial POST
(pre-stream) or as the terminal `error` event's `code` (in-stream):

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
internal timer, `delayElements`' pending timer, the WebClient's HTTP connection) —
verified live: disconnecting mid-`trigger:slow` logs `SSE_CLIENT_CANCELLED` at INFO
with no wasted downstream work, no ERROR-level noise.

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
                       │  ── MockMlAgentClient     │  ── HttpMlAgentClient (WebClient, pooled)
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
generic, client-safe string — never an exception message, stack trace, or
WebClient/hostname detail.

The ML Agent's own `tool_call`/`tool_result` events are consumed and logged (DEBUG)
by `HttpMlAgentClient`/`MockMlAgentClient` directly and **never** become a domain
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
`HttpMlAgentClient` validations, so this path is exercisable without a real agent);
`Flux.error(...)` after `Started` for `trigger:error`/`trigger:rejected`/
`trigger:continuation-expired`; `Flux.just(Started, Done(...))` for empty (echoing or
fabricating `conversationId`/`continuation`, same logic real Done always uses);
`Flux.concat(Mono.just(Started), Mono.never())` for timeout — the orchestrator's own
timeout operator is what ends that one, not the mock. `delayElements` natively
respects cancellation, so a disconnect mid-slow-stream stops everything downstream for
free.

**`HttpMlAgentClient`** — built from the shared, pooled `WebClient` (§7), consumes the
real ML Agent's six SSE event types via
`.bodyToFlux(ParameterizedTypeReference<ServerSentEvent<String>>)` plus a manual,
per-event-type Jackson deserialization step (each event type has a different JSON
shape, so one shared record wouldn't fit), never buffers the full response (no
`collectList()`/`.block()` anywhere — proven by the MockWebServer tests, not just
claimed), and validates every `payload`/`done`/`token` element (§16) before it
becomes a domain event. Translates the unified error-code space (§2) into
`MlAgentRejectedException`/`MlAgentContinuationExpiredException`/
`MlAgentCommunicationException` both pre-stream (the initial POST's HTTP status) and
in-stream (the terminal `error` event's `code`).

## 7. WebClient & Connection Pool Configuration

[`MlAgentWebClientConfig`](../src/main/java/com/cmbotservice/config/MlAgentWebClientConfig.java)
builds **one** shared `WebClient` bean (never per-request) from the **Boot-autoconfigured
`WebClient.Builder`** (injected, not `WebClient.builder()` directly — the autoconfigured
builder already carries Boot's Micrometer Observation instrumentation, which is what
gives the ML Agent call its own trace span for free, see §11/§14). A
`reactor.netty.resources.ConnectionProvider` sets max connections, pending-acquire
timeout, max idle time, and max connection lifetime; the `HttpClient` sets connect
timeout, response timeout, and keep-alive; codecs get an explicit
`maxInMemorySize`. Only built in `ml-agent.mode: http` (the mock has no network phase
to pool connections for). All values come from typed, `@Validated`
[`MlAgentProperties`](../src/main/java/com/cmbotservice/config/MlAgentProperties.java)
— a missing/invalid mandatory value fails startup, not a request.

## 8. Timeout Strategy — five concepts, three enforcement points

- **Connect timeout** / **response-header timeout** — reactor-netty `HttpClient`
  options in `MlAgentWebClientConfig`. Only meaningful for `HttpMlAgentClient`; the
  mock has no network phase, documented as such rather than faked.
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

Backpressure: this is a genuinely end-to-end reactive pipeline — the WebClient's
demand signals flow to the real ML Agent's TCP read window; the mock's
`delayElements`/`Flux.concat` construction only ever produces what's been requested.
That is the backpressure *strategy* to document, not a buffer size to tune: there is
no `Sinks.many()` or unbounded queue anywhere in this pipeline, confirmed by
inspection and called out explicitly since that's the one place these guarantees
quietly break if introduced carelessly.

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

`micrometer-tracing-bridge-otel` gives every request, and the ML Agent call, a real
span (the latter automatic, since the WebClient is built from the observation-
instrumented `WebClient.Builder` — see §7) — confirmed indirectly via
`http.server.requests` metrics in `/actuator/prometheus`, which only exist because an
observation is being created per request. No exporter is configured — that's
infrastructure, explicitly out of scope for this task. **Not yet working:** the
resulting trace/span IDs are not currently reaching MDC/logs — see §11's note on this;
it's a separate, still-open problem from the `tenantId`/`caseId`/`conversationId`
bridging fixed there.

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
deduplicated anywhere (no store to dedupe against — by design).

`spring.codec.max-in-memory-size` bounds the server-side request body; a separately
configured limit on the ML-agent `WebClient` (§7) bounds its response buffering — two
different knobs for two different directions. No speculative `metadata` map was added
to the request contract just to have size-limit annotations to attach to it — nothing
in the current contract needs one.

DTO boundary: `ChatRequest` (frontend) → `MlAgentRequest` (ML Agent, built explicitly
by `ChatOrchestrationService`, never the same object) → `MlAgentStreamEvent` (ML
Agent's response shape) → `ChatSseEvent`/`ServerSentEvent` (frontend again). Four
distinct types, one deliberate mapping step at each boundary — the frontend is never
exposed to the ML Agent's contract directly, so ML Agent contract changes are
absorbed here, not propagated.

## 16. ML Agent Response Validation

`HttpMlAgentClient` validates every decoded `ServerSentEvent<String>`: an unrecognized
`event` name, a missing required field for that event type (e.g. `token` without
`delta`), or a malformed JSON body all become `MlAgentMalformedResponseException`
(non-retryable, §9) instead of an uncontrolled `NullPointerException`/
`ClassCastException` escaping the mapping stage. Two additional, contract-specific
invariants are enforced on every `payload` event before it becomes a domain event
(§2): every `keySignal` must carry at least one citation, and
`suggestedResolution.mark` — when present — must be one of the known resolution codes
(`F S G A U Y B T X C`). Both violations also become `MlAgentMalformedResponseException`
— proven by dedicated MockWebServer tests, not just asserted.

## 17. Security Hardening (code-level; no gateway/infra here)

- `RequestContextResolver` isolates "who is calling" behind an interface — the
  `X-User-Id` header trust in the POC implementation is an explicit stand-in for a
  real authentication principal (JWT/session), swappable with no controller/service
  change.
- No stack trace, WebClient internal detail, or hostname ever reaches a response body
  — every error path funnels through either `GlobalExceptionHandler` (pre-stream) or
  the fixed `errorMessage` strings in `ChatOrchestrationService#toErrorEvent`
  (in-stream).
- Correlation IDs are logged; auth-adjacent header values are not.
- Content-type/body validation is WebFlux's own default behavior (confirmed, not
  re-implemented).

## 18. Configuration Management

All tunables are typed, `@Validated` `@ConfigurationProperties` records
(`MlAgentProperties`, `ResilienceProperties`, `ChatProperties`) — a missing or invalid
mandatory value fails application startup, never surfaces as a mysterious runtime
failure on the first request. `ml-agent.mode: mock|http` selects the `MlAgentClient`
bean via `@ConditionalOnProperty` on each implementation — orchestration code depends
only on the interface, never a mode check.

## 19. Package Structure

```
com.cmbotservice
 ├─ config/     MlAgentProperties, ResilienceProperties, ChatProperties,
 │               MlAgentWebClientConfig, ResilienceConfig, OpenApiConfig
 ├─ context/    RequestContext, RequestContextResolver (+HeaderBased impl),
 │               RequestHeaders, CorrelationIdFilter (WebFilter), MdcContext
 ├─ web/
 │   ├─ controller/  ChatController
 │   ├─ dto/         ChatRequest, ErrorResponse
 │   └─ advice/      GlobalExceptionHandler
 ├─ service/    ChatOrchestrationService, ChatMetrics
 ├─ mlagent/    MlAgentClient, MlAgentRequest, MlAgentStreamEvent, CaseSummaryPayload,
 │               MockMlAgentClient, MockScenario, HttpMlAgentClient
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

## Verified end-to-end

Full test suite (43 tests: unit, `StepVerifier`/virtual-time, MockWebServer,
`RestTestClient` against a real random port) green, including the real ML Agent
contract's request body shape, all six SSE event types, both `payload` invariant
validations, pre-stream and in-stream error-code mapping, and `continuation`/
`conversationId` round-tripping across two requests. Live curl verification: success/
slow/error/empty/rejected/continuation-expired scenarios; blank/missing-field validation → 400; circuit breaker
forced open via repeated `trigger:error` → subsequent request rejected instantly with
`CONCURRENCY_LIMIT_REACHED`, mock never re-invoked, `/actuator/health/readiness`
stays UP throughout; mid-stream client disconnect → `SSE_CLIENT_CANCELLED` at INFO,
zero ERROR-level noise; `/actuator/health`, `/actuator/health/{liveness,readiness}`,
`/actuator/prometheus`, `/actuator/circuitbreakers` all reachable and correct; a
`grep -rn "\.block()\|Thread.sleep" src/main/java` sanity check returns zero hits in
production code; `tenantId`/`caseId`/`conversationId` MDC fields populating correctly
end-to-end after the `Hooks.enableAutomaticContextPropagation()` fix (§11) —
re-verified with a fresh app start and real curl calls, not just the unit tests.
