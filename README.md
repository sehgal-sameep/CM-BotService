# CM-BotService — Case Manager Chatbot Backend

Java 21 / Spring **WebFlux** (fully non-blocking, Reactor) **stateless** integration
and orchestration layer between the Case Manager chatbot capability and the external
ML/AI Agent. Production-hardened for high traffic and a large number of concurrent,
long-lived SSE connections: circuit breaker, bulkhead, bounded retry, declarative
timeouts, true reactive backpressure, structured logging, metrics, and tracing.

```text
The Java backend does not own conversation memory.
The Java backend does not persist chatbot history.
The Java backend is a stateless streaming orchestrator.
```

This service **does not** implement any ML/LLM logic — see
[`MlAgentClient`](src/main/java/com/cmbotservice/mlagent/MlAgentClient.java),
[`MockMlAgentClient`](src/main/java/com/cmbotservice/mlagent/MockMlAgentClient.java)
(active by default) and
[`HttpMlAgentClient`](src/main/java/com/cmbotservice/mlagent/HttpMlAgentClient.java)
(real WebClient-based implementation, selected via config) for the abstraction the
real ML team's API plugs into.

The full architecture rationale (why WebFlux, timeout/retry/circuit-breaker/bulkhead
design, SSE contract, tracing/MDC mechanism, etc.) is in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — start there for the "why". For a
simple, non-code walkthrough of the request/response flow and payloads (frontend ↔
this backend ↔ ML Agent), see
[`docs/CHAT_API_GUIDE.md`](docs/CHAT_API_GUIDE.md) — hand that one to a frontend
developer or a new teammate.

## Running locally

```bash
./mvnw spring-boot:run          # Linux/macOS
mvnw.cmd spring-boot:run        # Windows
```

The app starts on `http://localhost:8080` with the mock ML Agent active by default
(`ml-agent.mode: mock`).

- Swagger UI: http://localhost:8080/swagger-ui.html
- OpenAPI JSON: http://localhost:8080/v3/api-docs
- Health: http://localhost:8080/actuator/health,
  `/actuator/health/liveness`, `/actuator/health/readiness`
- Metrics: http://localhost:8080/actuator/prometheus
- Circuit breaker state: http://localhost:8080/actuator/circuitbreakers

> **Swagger UI's "Try it out" cannot render a live SSE stream** — it waits for the
> connection to close, then shows the buffered body. Use the `curl -N` commands below
> to watch events arrive in real time.

## The one endpoint

`POST /api/v1/chat/messages` — send a message (predefined prompt or free text),
stream the ML Agent's response back as SSE. `tenantId`/`caseId`/`conversationId`/
`continuation` travel in the JSON body, not the URL — there's no backend-owned
resource to nest a path under.

Continuing a conversation is a pure echo, not a choice this backend makes: the real
ML Agent's own contract states the caller never needs to pick a resumption mechanism,
just send back whatever the previous response's `stream-complete` event returned.
Concretely, `conversationId` and `continuation` are both optional on the request —
omit both to start a new conversation, or send back either/both from a prior
`stream-complete` to continue one. **This backend never stores or interprets either
value** — the caller is responsible for remembering and resending them. `requestId`
is an optional caller-generated id forwarded for tracing/correlation only.
`endUserId` is an optional pass-through hint forwarded to the ML Agent's
`context.endUserId` — its exact semantics are defined by the ML Agent's own contract,
not this backend.

Headers `X-User-Id` and `X-Correlation-Id` are optional (see architecture doc §11).
Log/monitoring correlation for one chatbot interaction is handled entirely through
`correlationId`, `conversationId`, `tenantId`, and `caseId` — no separate client-owned
tracing header is needed.

### 1. Start a new conversation

```bash
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "X-User-Id: analyst-1" \
  -d '{"tenantId":"tenant-42","caseId":"case-1001","message":"Summarize this case for me"}'
```

The **only** authoritative place `conversationId`/`continuation` appear is the final
`stream-complete` event — the ML Agent reveals nothing about either one until the
response is fully done (an earlier `stream-start` echoes the request's own value, or
blank for a new conversation — see architecture doc §2 "conversation id known only at
done"). Capture both from `stream-complete` (`$CONV_ID`/`$CONTINUATION` below).

### 2. Continue the conversation

```bash
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -d "{\"tenantId\":\"tenant-42\",\"caseId\":\"case-1001\",\"conversationId\":\"$CONV_ID\",\"continuation\":\"$CONTINUATION\",\"message\":\"Which rules were triggered?\"}"
```

Free text works the same way, e.g. `"Why was this transaction considered suspicious?"`,
`"What should I investigate next?"`.

### 3. Exercise the mock ML Agent's failure modes

The mock recognizes these keywords anywhere in `message`:

```bash
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:slow"}'
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:timeout"}'   # first-response/idle timeout, then an `error` event
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:error"}'
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:empty"}'
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:rejected"}'               # -> errorCode NOT_FOUND, not retried
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:continuation-expired"}'   # -> errorCode CONTINUATION_EXPIRED, not retried
```

### 4. Request validation

```bash
curl -s -X POST "http://localhost:8080/api/v1/chat/messages" -H "Content-Type: application/json" -d '{"caseId":"c","message":"hi"}'
# -> 400 VALIDATION_ERROR: "tenantId: tenantId must not be blank"
```

### 5. Force the circuit breaker open (local demo)

With default config (`failure-rate-threshold: 50`, `minimum-number-of-calls: 5`),
five-ish consecutive `trigger:error` calls will open it:

```bash
for i in 1 2 3 4 5; do curl -s -o /dev/null -X POST "http://localhost:8080/api/v1/chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"trigger:error"}'; done
curl -s http://localhost:8080/actuator/circuitbreakers   # state: OPEN
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"hello"}'
# -> immediate `error` event, errorCode: CONCURRENCY_LIMIT_REACHED — the mock is never called
curl -s http://localhost:8080/actuator/health/readiness  # -> still UP
```

## SSE event contract (FE ↔ BE)

| Event | Payload | Cardinality |
|---|---|---|
| `stream-start` | `{ conversationId, messageId, timestamp }` | one, first — `conversationId` here is an **unconfirmed echo** of the request, not authoritative |
| `message` | `{ conversationId, messageId, sequence, content, timestamp }` | many — one streamed answer fragment (the ML Agent's own `token`/`delta`) |
| `payload` | `{ conversationId, messageId, payload, timestamp }` | exactly one, always before `stream-complete` — the structured, cited case analysis (`answer`, `summary.narrative/keySignals/entities/timeline`, `suggestedResolution`, `citations`) |
| `stream-complete` | `{ conversationId, continuation, messageId, totalChunks, timestamp }` | one, terminal — **the only authoritative source** of `conversationId`/`continuation`; echo both back on the next message |
| `error` | `{ conversationId, messageId, errorCode, message, timestamp }` | terminal |

The ML Agent's own `tool_call`/`tool_result` events (its internal tool-orchestration
trace) are consumed and logged (DEBUG) by `HttpMlAgentClient`/`MockMlAgentClient` and
**never** forwarded as an SSE event — this backend is the product, not the sandbox
the ML Agent's own docs describe those events as being rendered in.

## Resilience behavior

| Concern | Mechanism | Where |
|---|---|---|
| Connect / response-header timeout | reactor-netty `HttpClient` options | `MlAgentWebClientConfig` (only meaningful for `HttpMlAgentClient`) |
| First-response / idle-stream timeout | one `Flux.timeout(...)` operator, two independently configurable durations | `ChatOrchestrationService` |
| Total request timeout (absolute ceiling, active or not) | `withTotalDeadline` helper | `ChatOrchestrationService` |
| Circuit breaker | resilience4j `CircuitBreakerOperator`, re-checked on every retry | `ChatOrchestrationService` / `ResilienceConfig` |
| Bulkhead (max concurrent ML calls) | resilience4j `BulkheadOperator`, re-acquired on every retry | `ChatOrchestrationService` / `ResilienceConfig` |
| Retry | Reactor's own `Retry.backoff` (exponential + jitter); only for connection-level/transient failures, **never** once any content has streamed | `ChatOrchestrationService#isRetryable` |
| Cancellation | native `Flux` cancellation propagation on client disconnect — no polling, no manual cleanup | end-to-end |
| Backpressure | true reactive demand propagation (WebClient ↔ real ML Agent's TCP window; `delayElements`/pull-based mock) — no unbounded buffer anywhere | end-to-end |

Every failure that occurs **after** the SSE response has committed (200,
`text/event-stream`) — including a circuit-breaker rejection, a bulkhead rejection, a
timeout, or an ML Agent failure — surfaces as an `error` SSE event on the same stream,
never a different HTTP status; see architecture doc §4/§6.

## Health, readiness, and graceful shutdown

`/actuator/health/readiness` **stays UP even while the ML Agent circuit breaker is
open** — a downstream ML Agent outage is handled through the circuit breaker,
`error` events, and metrics, not by pulling healthy instances out of rotation (see
architecture doc §15 for why). The plain `/actuator/health` aggregate does reflect the
circuit breaker's own health indicator.

`server.shutdown: graceful` — on shutdown, the app stops accepting new connections and
gives in-flight requests/streams up to `spring.lifecycle.timeout-per-shutdown-phase`
(default 20s) to finish before closing.

## Console logging

Console output is color-coded (via Spring Boot's built-in Logback `%clr` converter, no
extra dependency) so a developer can scan logs quickly:

- **Level**: `ERROR` red, `WARN` yellow, `INFO`/`DEBUG` green
- Trace context `[corrId=...,tenant=...,case=...,conv=...,trace=...,span=...]`: cyan —
  lets you follow one chatbot interaction across lines at a glance, including its
  OpenTelemetry trace/span IDs
- Logger name: blue; timestamp/thread/separators: faint (de-emphasized)

These MDC fields are restored on every thread hop via Reactor's automatic context
propagation (`Hooks.enableAutomaticContextPropagation()`, enabled explicitly in
`MdcContext` — do not assume Boot turns this on by itself just because
`context-propagation` is on the classpath; it silently didn't in testing, which is
exactly what left every field blank until this was added explicitly).

Colors only appear on a real terminal — Spring Boot auto-detects this and silently
prints plain text when output is redirected to a file or picked up by a log
aggregator. Force it with:

```bash
./mvnw spring-boot:run -Dspring-boot.run.arguments=--spring.output.ansi.enabled=ALWAYS
```

Logical event names to grep for: `CHAT_REQUEST_RECEIVED`, `ML_REQUEST_STARTED`,
`ML_STREAM_STARTED`, `ML_STREAM_COMPLETED`, `ML_REQUEST_TIMEOUT`, `ML_REQUEST_FAILED`,
`ML_REQUEST_RETRY`, `SSE_CLIENT_CANCELLED`, `CIRCUIT_BREAKER_OPEN`,
`CONCURRENCY_LIMIT_REACHED`. Never logged: full prompts/responses (DEBUG-only,
truncated preview via `LogSanitizer`), auth headers, stack traces in responses.

## Metrics and tracing

`/actuator/prometheus` exposes (all low-cardinality — no `conversationId`/`caseId`/
`userId` tags, ever, by construction — see `ChatMetrics`):

```text
chat.requests.total / chat.requests.active
ml.requests.total{result=success|failed|timeout|rejected}
ml.stream.active / ml.stream.duration / ml.first_response.latency
sse.connections.active / .completed / .cancelled
resilience4j_circuitbreaker_state{name=mlAgent,state=...} / resilience4j_bulkhead_*
```

Tracing: `micrometer-tracing-bridge-otel` gives every request (and the ML Agent call)
a real trace/span ID, automatically bridged into MDC/logs — no exporter is configured
(that's infrastructure, out of scope here), so nothing leaves the process, but the IDs
are there in every log line for correlation. See architecture doc §11/§14 for the
mechanism (Micrometer's `ContextRegistry`, not raw `ThreadLocal`).

## Configuration

Fully typed, validated `@ConfigurationProperties` — a missing/invalid mandatory value
fails startup, not a request. See `src/main/resources/application.yml`:

| Property | Purpose |
|---|---|
| `ml-agent.mode` | `mock` (default) or `http` — selects the active `MlAgentClient` bean |
| `ml-agent.base-url` / `chat-path` | Only used in `mode: http` |
| `ml-agent.connect-timeout` / `response-timeout` | reactor-netty connection-level timeouts (`mode: http` only) |
| `ml-agent.first-response-timeout` / `idle-timeout` | Per-element `Flux` timeout, client-agnostic |
| `ml-agent.max-connections` / `pending-acquire-timeout` / `max-idle-time` / `max-life-time` | Connection pool (`mode: http` only) |
| `ml-agent.max-in-memory-size` | ML-agent WebClient's own response buffer limit |
| `resilience.circuit-breaker.*` | failure-rate-threshold, sliding-window-size, wait-duration-in-open-state, permitted-calls-in-half-open-state, minimum-number-of-calls |
| `resilience.bulkhead.*` | max-concurrent-calls, max-wait-duration |
| `resilience.retry.*` | max-attempts, initial-backoff, max-backoff, jitter-factor |
| `ml-agent.include-resolutions` | Server-side default for the ML Agent's `options.includeResolutions` — whether its `payload` event includes a `suggestedResolution` |
| `chat.max-message-length` | Runtime-checked ceiling (alongside the hard `@Size(max=4000)` on the DTO) |
| `chat.max-stream-duration` | Absolute per-request ML Agent call deadline |
| `app.mock-ml-agent.chunk-delay-ms` / `slow-chunk-delay-ms` | Mock-only chunk pacing |

## Tests

```bash
./mvnw test
```

- `MockMlAgentClientTest` — reactive scenario behavior via `StepVerifier` (incl.
  `withVirtualTime` for delay-shaped scenarios — no `Thread.sleep`), including the new
  `trigger:rejected`/`trigger:continuation-expired` scenarios and continuation/
  conversationId echo-vs-fabricate behavior.
- `ChatOrchestrationServiceTest` — retry classification (before/after first event, incl.
  `MlAgentRejectedException`/`MlAgentContinuationExpiredException` never retried),
  circuit breaker open, bulkhead rejection, total-deadline enforcement, message-length
  rejection — driven directly against small resilience4j instances, no Spring context.
- `HttpMlAgentClientTest` — MockWebServer: real request body shape assertion (matches
  the ML Agent's wire contract, omits internal-only fields), all six SSE event types
  including `tool_call`/`tool_result` being consumed silently, both `payload`
  invariant validations, pre-stream 401/403/404/422/429/503 → exception mapping,
  in-stream `error` event code mapping (incl. 4221/4222 → continuation expired).
- `ChatControllerTest` — full stack (`RestTestClient` against a real random port):
  SSE ordering (incl. the new `payload` event), validation, ML-failure-as-error-event,
  correlation ID echo, `continuation`/`conversationId` round-tripping across two
  requests, 404.

## Replacing the mock with the real ML Agent

Set `ml-agent.mode: http` and provide a real `ml-agent.base-url`/`chat-path` —
`HttpMlAgentClient` activates and `MockMlAgentClient` steps aside automatically
(`@ConditionalOnProperty` on both). No controller or `ChatOrchestrationService` change
required. `HttpMlAgentClient` now speaks the real, documented `POST /v1/chat` contract
(Thoughtful Labs) — see the SSE event contract table above and `docs/ARCHITECTURE.md
§2` for the full request/response shape, including the two payload invariants it
validates (every key signal needs a citation; `suggestedResolution.mark` must be a
known code).

## Statelessness and horizontal scaling

This service holds **no conversation state** in memory or in any database between
requests — no conversation store, no chat history, no per-conversation session
affinity, no sticky sessions. Any instance can handle any request for any
conversation. Combined with WebFlux's non-blocking I/O model, this lets the service
scale horizontally under a large number of concurrent long-lived SSE connections
without the per-connection thread cost a traditional blocking server would incur. If
audit/history requirements emerge later, they belong in an external mechanism
(logging/metrics/tracing, or a dedicated audit service) — not in this orchestration
layer.

## Known limitations

- The real contract's `contextToken`/product-auth path isn't wired up — this backend
  currently always sends the sandbox-path `tenantId`, since there's no real auth/JWT
  infrastructure yet. Documented gap, not a bug; see `docs/ARCHITECTURE.md §2`.
- `context.endUserId`'s exact semantics (the analyst vs. the case's own customer)
  aren't pinned down by the ML Agent's contract yet — this backend forwards whatever
  `ChatRequest.endUserId` supplies verbatim, `null` if omitted, without guessing.
- `trace=`/`span=` currently show blank in every log line despite
  `micrometer-tracing-bridge-otel` being on the classpath and `http.server.requests`
  metrics confirming request observations *are* being created — so a span exists, but
  its trace/span IDs aren't reaching MDC. This is a different, still-open issue from
  the one fixed above (which was about our own `tenantId`/`caseId`/`conversationId`
  keys, via our own registered accessors) — Micrometer Tracing's own MDC bridging
  isn't working yet and hasn't been root-caused.
- `conv=` in MDC stays blank for the whole request, even after the ML Agent resolves a
  conversation id on `Done` — the context written at subscription time can't
  retroactively pick up a value learned mid-stream without an explicit `MDC.put` at
  that point, which isn't done yet. The resolved id is still correct everywhere it
  actually matters (the `stream-complete` SSE event itself), just not backfilled into
  the log context.
- No exporter is configured for tracing (infrastructure, deliberately out of scope) —
  moot until the MDC bridging issue above is resolved anyway.
- Idempotency (`requestId`) is propagated and logged but not deduplicated anywhere —
  by design, since this service holds no state to deduplicate against.
- Individual ML response chunk size isn't independently bounded beyond the WebClient's
  overall `max-in-memory-size` — a concrete per-chunk limit would need the real ML
  Agent's contract to be meaningful.
