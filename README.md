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
[`GrpcMlAgentClient`](src/main/java/com/cmbotservice/mlagent/GrpcMlAgentClient.java)
(real gRPC-based implementation, selected via config) for the abstraction the real ML
team's API plugs into. Service-to-service communication with the ML Agent is gRPC
(see `src/main/proto/{common,case_manager,chat_agent}.proto`) — the frontend-facing
contract above it stays plain HTTP + SSE either way, since the two are entirely
decoupled by this interface.

The full architecture rationale (why WebFlux, timeout/retry/circuit-breaker/bulkhead
design, SSE contract, tracing/MDC mechanism, etc.) is in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — start there for the "why". For a
simple, non-code walkthrough of the request/response flow and payloads (frontend ↔
this backend ↔ ML Agent), see
[`docs/CHAT_API_GUIDE.md`](docs/CHAT_API_GUIDE.md) — hand that one to a frontend
developer or a new teammate. For copy-paste curl commands for every endpoint (use these
rather than Swagger's "Try it out", which can't send the session cookie or show a live
stream), see [`docs/API_CURL_REFERENCE.md`](docs/API_CURL_REFERENCE.md).

## Project setup & build

### Prerequisites

- **JDK 21** (Temurin/OpenJDK/Corretto — any distribution). Verify with `java -version`.
- **No local Maven install needed** — this repo ships the Maven Wrapper
  (`mvnw`/`mvnw.cmd`), which downloads the exact Maven version the project was built
  against on first use. Use the wrapper, not a system `mvn`, so everyone builds with
  the same Maven version.
- **Internet access on first build** — beyond the usual dependency downloads, the
  `protobuf-maven-plugin` (bound to `generate-sources`) downloads a per-OS `protoc`
  and `protoc-gen-grpc-java` binary the first time (resolved automatically for
  Windows/macOS/Linux via `os-maven-plugin`, see `pom.xml`). Subsequent builds reuse
  the cached binaries from `~/.m2/repository`.
- **Git**, to clone the repo.
- Nothing else is required to build and run the app with its defaults
  (`ml-agent.mode: mock`, `chatbot.security.mode: NONE`) — no local Redis, no real ML
  Agent endpoint, no database.

### Build

```bash
./mvnw clean install      # Linux/macOS — compiles, generates gRPC/protobuf sources,
mvnw.cmd clean install    # Windows      runs tests, installs the jar to ~/.m2

./mvnw clean package -DskipTests    # build only, skip tests (e.g. for a quick local jar)
```

Generated protobuf/gRPC Java sources land in
`target/generated-sources/protobuf/{java,grpc-java}` — if your IDE doesn't pick them
up automatically after a first build (imports on `ChatAgentGrpc`, request/response
message classes unresolved), mark that directory as a generated-sources root, or
re-run `mvn generate-sources` and refresh/reimport the Maven project.

### Run

```bash
./mvnw spring-boot:run          # Linux/macOS
mvnw.cmd spring-boot:run        # Windows

# or, after a `clean package`:
java -jar target/CM-BotService-0.0.1-SNAPSHOT.jar
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

All controller endpoints are served under a common base path, `chatbot.api.base-path`
(env `API_BASE_PATH`, default `/back-office-ai`); actuator and Swagger are not prefixed.

`POST /back-office-ai/api/v1/chat/messages` — send a message (predefined prompt or free text),
stream the ML Agent's response back as SSE. `tenantId`/`organization` travel as the
required `X-Tenant-Id`/`X-Org-Id` request headers (not the body); `caseId`/
`history` travel in the JSON body, not the URL — there's no backend-owned resource to
nest a path under.

The real ML Agent's contract has **no conversation/continuation token at all** —
resending the full `history` transcript (oldest turn first) on every message is the
only resumption mechanism. `history` is optional (omit or send empty to start a new
conversation) and, if sent, is an array of `{role, content}` turns, forwarded to the
ML Agent (translated into its own `user`/`agent` turn shape — see
`GrpcMlAgentClient#toConversationTurn`). **This backend never stores, assembles, or
interprets the transcript** — the caller is responsible for remembering and resending
it. `requestId` is an optional caller-generated id forwarded for tracing/correlation
only. `endUserId` is an optional pass-through hint forwarded to the ML Agent's case
context — this backend does not interpret it.

`X-Tenant-Id` and `X-Org-Id` are **required** headers — missing or blank
either one rejects the request with `400 VALIDATION_ERROR` before it ever reaches the
ML Agent. `X-User-Id` and `X-Correlation-Id` are optional (see architecture doc §11).
Log/monitoring correlation for one chatbot interaction is handled entirely through
`correlationId`, `tenantId`, and `caseId` — no separate client-owned tracing header is
needed.

### 1. Start a new conversation

```bash
curl -N -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "X-User-Id: analyst-1" \
  -H "X-Tenant-Id: tenant-42" \
  -H "X-Org-Id: org-7" \
  -d '{"caseId":"case-1001","message":"Summarize this case for me"}'
```

The response is the ML Agent's own event stream, untranslated — see the SSE event
contract below. `done` carries no identifier to capture; its `stop_reason` tells you
whether the agent cut generation short (`STOP_REASON_TRUNCATED`).

### 2. Continue the conversation

Resend the transcript so far as `history`, appended with the new message:

```bash
curl -N -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "X-Tenant-Id: tenant-42" \
  -H "X-Org-Id: org-7" \
  -d '{"caseId":"case-1001","history":[{"role":"user","content":"Summarize this case for me"},{"role":"assistant","content":"..."}],"message":"Which rules were triggered?"}'
```

Free text works the same way, e.g. `"Why was this transaction considered suspicious?"`,
`"What should I investigate next?"`.

### 3. Exercise the mock ML Agent's failure modes

The mock recognizes these keywords anywhere in `message`:

```bash
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:slow"}'
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:timeout"}'   # first-response timeout, then a `service_error` event
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:error"}'     # transport failure: retried, then `service_error` ML_AGENT_ERROR
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:agent-error"}' # the ML Agent's own `error` event, forwarded as-is
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:empty"}'
curl -N -X POST ".../chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:rejected"}'               # -> `service_error` errorCode NOT_FOUND, not retried
```

### 4. Request validation

```bash
curl -s -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"bad id!","message":"hi"}'
# -> 400 VALIDATION_ERROR: "caseId: caseId may only contain letters, digits, '_' and '-'" (caseId itself is optional)

curl -s -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" -H "Content-Type: application/json" -H "X-Org-Id: o" -d '{"caseId":"c","message":"hi"}'
# -> 400 VALIDATION_ERROR: "X-Tenant-Id header must not be blank" (missing entirely, same result)
```

### 5. Force the circuit breaker open (local demo)

With default config (`failure-rate-threshold: 50`, `minimum-number-of-calls: 5`), a
couple of `trigger:error` calls will open it. Each call makes up to 4 attempts (1 + 3
retries, since nothing has streamed yet), and every attempt counts toward the breaker:

```bash
for i in 1 2 3 4 5; do curl -s -o /dev/null -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"trigger:error"}'; done
curl -s http://localhost:8080/actuator/circuitbreakers   # state: OPEN
curl -N -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"hello"}'
# -> immediate `service_error` event, errorCode: CONCURRENCY_LIMIT_REACHED — the mock is never called
curl -s http://localhost:8080/actuator/health/readiness  # -> still UP
```

## Authentication

`chatbot.security.mode` is the master toggle, checked before the request ever reaches
`ChatController`:

- **`NONE`** (default here, local-dev only) — bypasses all authentication; every
  request is permitted unchanged. Logs a loud `WARN` banner on every startup while
  active, so it's impossible to miss in logs if accidentally left on.
- **`BFF_SESSION`** (the only mode for any shared/prod deployment) — validates an
  existing BFF-issued session, read-only, against the same Redis instance FMC-PM-BFF
  uses. **Current scope is deliberately minimal**: `sessionId + tenant → Redis lookup →
  session found → extract accessToken → authenticated → proceed`. Extract the session
  cookie and the (mandatory) `X-Tenant-Id` header → look up `session:<sessionId>:<tenant>`
  (never write/refresh/delete) → a record found at all is trusted as authenticated,
  full stop. There is currently **no** fingerprint, CSRF, permission, or token-expiry
  check, even though the source design diagram (`docs/chatbot_auth_redis_lookup.png`)
  calls for all four — see `docs/ARCHITECTURE.md` §20 if that scope needs to come back.
  The session's `access_token` is carried onto `RequestContext.accessToken`, ready for
  a future call to the TFLabs Orchestrator Service (not yet made). Rejection returns
  the same `ErrorResponse` shape used elsewhere in this API (`401 UNAUTHENTICATED` if
  no session record exists, `503 SESSION_STORE_UNAVAILABLE` if Redis itself is
  unreachable).

Only `POST /api/v1/chat/messages` is actually gated — actuator health/readiness,
Swagger UI, and the OpenAPI JSON stay reachable without a session either way, since
a k8s liveness/readiness prober has no BFF session cookie to send.

```bash
# mode: NONE (default) — works with no cookie at all
curl -N -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" -d '{"caseId":"c","message":"hello"}'

# mode: BFF_SESSION — needs only a session record to exist in Redis for this cookie+tenant
curl -N -X POST "http://localhost:8080/back-office-ai/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Cookie: SESSION=<value>" \
  -H "X-Tenant-Id: t" \
  -H "X-Org-Id: o" \
  -d '{"caseId":"c","message":"hello"}'
```

| Property | Purpose |
|---|---|
| `chatbot.security.mode` | `NONE` (default) or `BFF_SESSION` — the master toggle |
| `chatbot.security.session.cookie-name` | The BFF session cookie's name (default `SESSION`) |
| `chatbot.security.session.tenant-header-name` | Header carrying the tenant for the Redis lookup (default `X-Tenant-Id`, reusing the same always-required header — kept as its own property in case of future divergence) |
| `chatbot.security.redis.namespace` / `.strategy` / `.field-names.*` | Key prefix (default `session`, giving `session:<sessionId>:<tenant>`), the `SessionStore` bean to select, and the session envelope's field names (`context-json`/`access-token`/`refresh-token`/`fingerprint`, default `context_json`/`access_token`/`refresh_token`/`fp`) |
| `spring.data.redis.host` / `.port` / `.ssl.enabled` / `.password` | The shared Redis connection itself (Boot-managed, not `chatbot.security.*`) — local default `localhost:6379`, no TLS |
| `spring.data.redis.azure.passwordless-enabled` | `true` in a shared/prod environment: authenticates to Azure Cache for Redis via Entra ID/managed identity instead of a password (`spring-cloud-azure-starter-data-redis-lettuce`) |
| `chatbot.security.cors.allowed-origins` (`CORS_ALLOWED_ORIGINS`) | CORS, in every security mode. Default `*` (also: unset/empty) allows **any** origin with credentials, so browsers never get a CORS error. Set a comma-separated list of origins/patterns (e.g. `https://*.example.com,http://localhost:*`) to restrict it. |
| `chatbot.security.fail-open-on-redis-error` | Insecure local-dev-only escape hatch (default `false`): permit the request through, unauthenticated, if Redis is unreachable instead of rejecting with 503 |

See `docs/ARCHITECTURE.md`'s Authentication section for the full flow rationale and
every judgment call this implementation made against an intentionally
not-fully-specified design.

### Temporary debug endpoint: verifying the Redis lookup directly

`GET /api/v1/debug/session-lookup` (`SessionDebugController`, only registered in
`mode: BFF_SESSION`) reuses the exact same `SessionStore.findSession(...)` call the
real auth flow uses, and returns the raw session record instead of gating a chat
request — a quick way to confirm the Redis lookup itself works, from Swagger UI, with
no chat payload involved. **Not part of this service's stable API** — it's a
verification tool for the current minimal flow, meant to be removed once no longer
needed.

**Swagger UI's "Try it out" can't send the `SESSION` cookie.** Browsers forbid page
JavaScript from setting the `Cookie` header, so the request goes out without it and
returns `400 VALIDATION_ERROR: Required cookie 'SESSION' is not present.` Use curl (below),
or set the cookie on the Swagger page's origin first from DevTools
(`document.cookie = "SESSION=<value>; path=/"`). See
[`docs/SESSION_DEBUG_API.md`](docs/SESSION_DEBUG_API.md) for full instructions,
including Windows PowerShell (`curl.exe`, not `curl`).

```bash
curl "http://localhost:8080/back-office-ai/api/v1/debug/session-lookup" -H "Cookie: SESSION=<value>" -H "X-Tenant-Id: t"
```

Returns `200` with `{ username, tenantId, accessToken, refreshToken, contextJson,
fingerprint }` (every field exactly as stored, unvalidated) if a record exists, `401
UNAUTHENTICATED` if not, `503 SESSION_STORE_UNAVAILABLE` if Redis is unreachable.

## SSE event contract (FE ↔ BE)

**The ML Agent's response is streamed to the frontend as-is.** This backend doesn't
rename events or fields, reshape payloads, or add a wrapper. Each Thoughtful Labs
`AnswerEvent` (`src/main/proto/chat_agent.proto`) becomes one SSE event:

- `event:` is the name of the `AnswerEvent` oneof field that is set: `chunk`,
  `tool_call`, `tool_result`, `payload`, `done`, `error`, `ping`. It's read from the
  protobuf descriptor, not hard-coded, so a new arm added to the contract flows
  through under its own name.
- `data:` is the whole `AnswerEvent` in the canonical proto3 JSON mapping
  (`JsonFormat`), with the **original `.proto` field names and nesting**, enums as
  their proto names, and fields at their default value still printed. Per that mapping,
  `int64` fields are JSON strings.
- `id:` is a 0-based frame counter, the only thing this backend adds (SSE transport).

```
event:tool_call
data:{"tool_call":{"tool_call_id":"t-1","name":"getCase","args_json":"{\"caseId\":\"case-1001\"}"}}

event:tool_result
data:{"tool_result":{"tool_call_id":"t-1","status":"STATUS_OK","ms":"42","row_count":"7"}}

event:chunk
data:{"chunk":{"delta":"This case was..."}}

event:payload
data:{"payload":{"case_manager_answer_payload":{"key_signals":[{"signal":"...","citations":["evt-123"]}],"citations":[{"id":"evt-123","source":"getCase","fields":["risk_score"]}]}}}

event:done
data:{"done":{"stop_reason":"STOP_REASON_COMPLETED","latency_ms":"1800","tokens_in":"12","tokens_out":"140"}}
```

Every ML Agent event type is forwarded, including `ping` (a keepalive, harmless to
ignore) and the agent's own model-level `error` (`{"error":{"code":...,"retryable":...}}`),
which is never converted into a backend error. The only agent event not forwarded is
one with its `event` oneof unset: the contract says to ignore it, and it carries
nothing recognisable.

| Event | Source | Payload |
|---|---|---|
| `chunk`, `tool_call`, `tool_result`, `payload`, `done`, `error`, `ping` | ML Agent, verbatim | the `AnswerEvent` as proto3 JSON (see above) |
| `service_error` | this backend | `{ messageId, errorCode, errorMessage, timestamp }`, terminal. Only for failures that produced no agent event: unreachable/timed-out/gRPC-rejected agent, circuit breaker, bulkhead, message-length check |

Every stream ends with exactly one of `done`, `error`, or `service_error`. The full
frontend-facing walkthrough is in [`docs/CHAT_API_GUIDE.md`](docs/CHAT_API_GUIDE.md)
§5–§8.

## Resilience behavior

| Concern | Mechanism | Where |
|---|---|---|
| First-response / idle-stream timeout | one `Flux.timeout(...)` operator, two independently configurable durations, client-agnostic | `ChatOrchestrationService` |
| Total request timeout (absolute ceiling, active or not) | `withTotalDeadline` helper | `ChatOrchestrationService` |
| Circuit breaker | resilience4j `CircuitBreakerOperator`, re-checked on every retry | `ChatOrchestrationService` / `ResilienceConfig` |
| Bulkhead (max concurrent ML calls) | resilience4j `BulkheadOperator`, re-acquired on every retry | `ChatOrchestrationService` / `ResilienceConfig` |
| Retry | Reactor's own `Retry.backoff` (exponential + jitter); only for connection-level/transient failures, **never** once any content has streamed | `ChatOrchestrationService#isRetryable` |
| Cancellation | native `Flux` cancellation propagation on client disconnect, bridged all the way down to a gRPC `ClientCallStreamObserver#cancel(...)` | end-to-end |
| Backpressure | true reactive demand propagation (gRPC's own manual flow control ↔ Reactor demand; `delayElements`/pull-based mock) — no unbounded buffer anywhere | end-to-end |

No separate connect/response-header timeout knob exists at the gRPC layer — no
client-side gRPC deadline is set at all (`GrpcMlAgentClient`), so the
first-response/idle timeout above is the single, client-agnostic timeout authority
(see `docs/ARCHITECTURE.md §8`).

Every failure that occurs **after** the SSE response has committed (200,
`text/event-stream`) and has no ML Agent event of its own — a circuit-breaker
rejection, a bulkhead rejection, a timeout, or a transport-level ML Agent failure —
surfaces as a `service_error` SSE event on the same stream, never a different HTTP
status; see architecture doc §4/§6. The ML Agent's own `error` event is simply
forwarded.

## Health, readiness, and graceful shutdown

`/actuator/health/readiness` **stays UP even while the ML Agent circuit breaker is
open** — a downstream ML Agent outage is handled through the circuit breaker,
`service_error` events, and metrics, not by pulling healthy instances out of rotation (see
architecture doc §15 for why). The plain `/actuator/health` aggregate does reflect the
circuit breaker's own health indicator.

`server.shutdown: graceful` — on shutdown, the app stops accepting new connections and
gives in-flight requests/streams up to `spring.lifecycle.timeout-per-shutdown-phase`
(default 20s) to finish before closing.

## Console logging

Console output is color-coded (via Spring Boot's built-in Logback `%clr` converter, no
extra dependency) so a developer can scan logs quickly:

- **Level**: `ERROR` red, `WARN` yellow, `INFO` green
- Trace context `[corrId=...,tenant=...,case=...,trace=...,span=...]`: cyan —
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

Every API request is logged at each step, at INFO for normal progress, WARN for an
expected rejection or degraded path, and ERROR for a failure. There are **no DEBUG or
TRACE statements**, so the default `com.cmbotservice` level is `INFO`
(`LOG_LEVEL_COM_CMBOTSERVICE`; set `WARN` to see only problems). Actuator, Swagger UI,
and OpenAPI paths are not request-logged, so health probes don't bury real traffic.

Filter by correlation ID to get one request's whole trail. Send
`X-Correlation-Id: <id>`, or read the one echoed on the response, then
`grep "corrId=<id>"`. A chat request logs, in order: `HTTP_REQUEST_RECEIVED` →
(`BFF_SESSION`) `AUTH_CHECK_STARTED`, `REDIS_SESSION_LOOKUP_STARTED`,
`REDIS_SESSION_RECORD_FOUND`/`_PARSED`, `AUTH_SUCCEEDED` → `REQUEST_CONTEXT_RESOLVED` →
`CHAT_REQUEST_RECEIVED` → `GRPC_CALL_STARTED` (target host:port) or
`MOCK_ML_AGENT_CALL_STARTED` → `ML_REQUEST_STARTED` → `ML_STREAM_STARTED` →
`ML_EVENT_TOOL_CALL`/`_TOOL_RESULT`/`_PAYLOAD`/`_DONE`/`_ERROR` → `ML_STREAM_COMPLETED`
(per-event-type counts; `chunk`/`ping` are counted rather than logged one by one) →
`SSE_STREAM_COMPLETED` → `HTTP_REQUEST_COMPLETED` (status, duration).

WARN/ERROR events: `AUTH_REJECTED`, `REDIS_SESSION_RECORD_NOT_FOUND`,
`REDIS_SESSION_RECORD_INCOMPLETE`/`_UNPARSEABLE`, `REDIS_SESSION_LOOKUP_FAILED`
(Redis endpoint, full cause chain, stack trace), `GRPC_CALL_FAILED` (gRPC status and
description), `GRPC_EVENT_IGNORED`, `GRPC_PAYLOAD_CONTRACT_VIOLATION`,
`ML_REQUEST_RETRY`, `ML_REQUEST_TIMEOUT`, `CIRCUIT_BREAKER_OPEN`,
`CONCURRENCY_LIMIT_REACHED`, `ML_AGENT_REJECTED`, `ML_REQUEST_FAILED`,
`SSE_SERVICE_ERROR_SENT`, `REQUEST_VALIDATION_FAILED`, `REQUEST_REJECTED`. At startup:
`REDIS_SESSION_STORE_CONFIGURED`, then (in the background, non-fatal)
`REDIS_SESSION_STORE_REACHABLE` or `REDIS_SESSION_STORE_UNREACHABLE` with the cause chain,
and `ML_AGENT_GRPC_CHANNEL_CONFIGURED` or `ML_AGENT_MOCK_CONFIGURED`.

Never logged: prompt, history, or answer text (only lengths and counts, since case text
may contain personal data), access/refresh tokens, the Redis password, or the raw
session ID (only masked by `LogSanitizer.maskSecret`, e.g. `****cdef(len=29)`).

## Metrics and tracing

`/actuator/prometheus` exposes (all low-cardinality — no `caseId`/`userId` tags, ever,
by construction — see `ChatMetrics`):

```text
chat.requests.total / chat.requests.active
ml.requests.total{result=success|failed|timeout|rejected}   # failed includes streams ending in the agent's own `error` event
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
| `ml-agent.mode` | `mock` (default) or `grpc` — selects the active `MlAgentClient` bean |
| `ml-agent.grpc-host` / `grpc-port` | Only used in `mode: grpc` — the ML Agent's gRPC endpoint |
| `ml-agent.first-response-timeout` / `idle-timeout` | Per-element `Flux` timeout, client-agnostic (no separate gRPC deadline exists) |
| `ml-agent.grpc-max-inbound-message-size` | Max size of one message the gRPC channel will accept (`mode: grpc` only) |
| `resilience.circuit-breaker.*` | failure-rate-threshold, sliding-window-size, wait-duration-in-open-state, permitted-calls-in-half-open-state, minimum-number-of-calls |
| `resilience.bulkhead.*` | max-concurrent-calls, max-wait-duration |
| `resilience.retry.*` | max-attempts, initial-backoff, max-backoff, jitter-factor |
| `chat.max-message-length` | Runtime-checked ceiling (alongside the hard `@Size(max=4000)` on the DTO) |
| `chat.max-stream-duration` | Absolute per-request ML Agent call deadline |
| `app.mock-ml-agent.chunk-delay-ms` / `slow-chunk-delay-ms` | Mock-only chunk pacing |

## Tests

```bash
./mvnw test
```

- `SseEventsTest` — pins the wire format: every event name is the agent's own oneof
  field name (and every arm of the contract is covered), `data` keeps the `.proto` field
  names/nesting/enum names exactly, default-valued fields are never dropped, `data`
  round-trips through `JsonFormat.parser()` back to the identical proto message, and
  `service_error` is the only backend-defined event.
- `MockMlAgentClientTest` — reactive scenario behavior via `StepVerifier` (incl.
  `withVirtualTime` for delay-shaped scenarios — no `Thread.sleep`): the success path
  emits the real contract's `AnswerEvent`s (`tool_call`/`tool_result` trace, `chunk`s,
  `payload`, `done`), `trigger:agent-error` emits the agent's own `error` event, and
  `trigger:error`/`trigger:rejected` fail the `Flux` like a gRPC status would.
- `ChatOrchestrationServiceTest` — every agent event forwarded in order with identical
  name/data and nothing added or removed; the agent's `error` event forwarded as-is, not
  retried, not turned into a `service_error`; retry classification (before/after first
  event, `MlAgentRejectedException` never retried), circuit breaker open, bulkhead
  rejection, total-deadline enforcement, message-length rejection, and `history`
  forwarded to `MlAgentRequest` untouched — driven directly against small resilience4j
  instances, no Spring context.
- `GrpcMlAgentClientTest` — in-process gRPC server (the gRPC analog of MockWebServer):
  real request field mapping assertion (matches the ML Agent's proto contract,
  including `history`'s translation into the `user`/`agent` oneof); every event type,
  `ping` included, emitted as the **identical** proto message the server sent (deep
  `equals`); `STATUS_UNSPECIFIED`/missing `row_count` not normalised; a `payload`
  violating the citation invariant still forwarded; in-stream `error` events forwarded
  rather than turned into exceptions; an unset-oneof event ignored per the contract;
  gRPC `Status.Code` → exception mapping.
- `ChatControllerTest` — full stack (`RestTestClient` against a real random port):
  the raw SSE body carries the agent's event names and snake_case field names/nesting
  and none of the former backend-owned names; event ordering; the agent's `error` event
  on the wire untouched; transport failure as `service_error`; validation, correlation
  ID echo, a request carrying `history` streaming normally, 404.

## Docker

A multi-stage `Dockerfile` is provided — stage 1 builds the jar with the Maven Wrapper
(JDK 21, also generates the gRPC/protobuf sources), stage 2 runs it on a minimal JRE
21 image as a non-root user, with a `curl`-based `HEALTHCHECK` against
`/actuator/health/liveness`.

```bash
docker build -t cm-bot-service .
docker run -p 8080:8080 cm-bot-service

# override any application.yml property via Spring Boot's relaxed env-var binding —
# no image rebuild needed, e.g. to point at a real ML Agent and enable BFF auth:
docker run -p 8080:8080 \
  -e ML_AGENT_MODE=grpc -e ML_AGENT_GRPC_HOST=ml-agent -e ML_AGENT_GRPC_PORT=9090 \
  -e CHATBOT_SECURITY_MODE=BFF_SESSION -e REDIS_HOST=redis \
  -e JAVA_OPTS="-Xmx512m -Xms256m" \
  cm-bot-service

# ...or, against a real Azure Cache for Redis with Entra ID/managed-identity auth
# (no password/connection string anywhere in this config):
docker run -p 8080:8080 \
  -e CHATBOT_SECURITY_MODE=BFF_SESSION \
  -e REDIS_HOST=redis-fmc-prod.redis.cache.windows.net -e REDIS_PORT=6380 \
  -e REDIS_SSL=true -e REDIS_AZURE_PASSWORDLESS_ENABLED=true \
  cm-bot-service
```

The image ships with no default Redis/ML-Agent connectivity baked in — with zero
environment overrides it boots exactly like `mode: mock` / `chatbot.security.mode:
NONE` locally (see "Running locally" above). `docker build` needs network access
(dependency + protoc downloads); the running container does not, beyond whatever
downstream services (`ml-agent.mode: grpc`, `chatbot.security.mode: BFF_SESSION`) you
point it at.

## Replacing the mock with the real ML Agent

Set `ml-agent.mode: grpc` and provide the real `ml-agent.grpc-host`/`grpc-port` —
`GrpcMlAgentClient` activates and `MockMlAgentClient` steps aside automatically
(`@ConditionalOnProperty` on both). No controller or `ChatOrchestrationService` change
required — this is the payoff of `MlAgentClient` being a real seam: the transport
underneath it (HTTP+SSE, then gRPC) has changed twice now, and the gRPC contract
itself has already changed once (`Chat`/`ChatRequest`/`ChatEvent` → `AskCaseManager`/
`AskCaseManagerRequest`/`AnswerEvent`), without either `ChatOrchestrationService` or
`ChatController` noticing. `GrpcMlAgentClient` speaks the ML Agent's `ChatAgent.
AskCaseManager` RPC (`src/main/proto/{common,case_manager,chat_agent}.proto`, Thoughtful
Labs' own finalized 3-file contract) — see the SSE event contract above and
`docs/ARCHITECTURE.md §2` for the full request/response shape. Because the response is
forwarded verbatim, a change to the ML Agent's response messages reaches the frontend
automatically once the `.proto` files here are updated, with no Java mapping to change.

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

- **`ChatRequest.history`'s wire shape is now confirmed** — Thoughtful Labs' finalized
  contract represents each turn as a `ConversationTurn` `user`/`agent` oneof
  (`common.proto`), not a `role`/`content` pair. This backend keeps `role`/`content` as
  its own stable internal/DTO shape (`ChatRequest`/`MlAgentRequest`'s
  `HistoryTurn`) and translates it at the boundary (`GrpcMlAgentClient
  #toConversationTurn`: `role == "user"` → a user turn, anything else → an agent
  turn) — a deliberate translation, not a lingering guess.
- **`AgentRequestContext.organization` now has a source: the required `X-Org-Id`
  request header**, read by `RequestContextResolver` into `RequestContext.organization`
  and forwarded to the ML Agent as-is — this backend does not look it up, validate it
  against the session's org lists (`context_json.mappedOrgs`/`grantedOrgs`, in `BFF_SESSION`
  mode — not consumed here at all, since nothing downstream needs them), or otherwise
  interpret it.
- **`operatorId` is sourced from the existing analyst identity** (`RequestContext
  .userId()` — the `X-User-Id` header in `mode: NONE`, or the BFF session username in
  `mode: BFF_SESSION`), the same value this backend already had available; nothing new
  had to be added upstream for this field.
- **The Redis session layout is now confirmed**, resolving what was previously an
  unconfirmed assumption: one JSON document per session at key
  `session:<sessionId>:<tenant>`, shaped as `{context_json, access_token,
  refresh_token, fp}` — `context_json` is itself a JSON *string* (not a nested object)
  carrying `username`/`tenantId`. `JsonBlobSessionStore` implements exactly this; a
  structurally different real layout would still need a new `SessionStore`
  implementation (behind `chatbot.security.redis.strategy`), not a config change.
- **Authentication scope was deliberately reduced from the diagrammed flow, by
  explicit request**: a Redis session found for `sessionId + tenant` is trusted as
  authenticated outright. The diagram's fingerprint check, separate CSRF-token key
  (`csrf:<sessionId>:<tenant>`) compared against `X-XSRF-TOKEN`, `CHATBOT_`-permission
  filter, and JWT `exp` expiry check were all previously implemented and then removed —
  not unconfirmed or forgotten, a scope decision. See `docs/ARCHITECTURE.md` §20.
- `trace=`/`span=` currently show blank in every log line despite
  `micrometer-tracing-bridge-otel` being on the classpath and `http.server.requests`
  metrics confirming request observations *are* being created — so a span exists, but
  its trace/span IDs aren't reaching MDC. This is a different, still-open issue from
  the one fixed above (which was about our own `tenantId`/`caseId` keys, via our own
  registered accessors) — Micrometer Tracing's own MDC bridging isn't working yet and
  hasn't been root-caused.
- No exporter is configured for tracing (infrastructure, deliberately out of scope) —
  moot until the MDC bridging issue above is resolved anyway.
- Idempotency (`requestId`) is propagated and logged but not deduplicated anywhere —
  by design, since this service holds no state to deduplicate against.
- Individual ML response chunk size isn't independently bounded beyond the gRPC
  channel's overall `grpc-max-inbound-message-size` — a concrete per-chunk limit would
  need the real ML Agent's contract to be meaningful.
- No TLS is configured on the gRPC channel (`GrpcChannelConfig` uses plaintext) —
  deliberately out of scope, same as the tracing exporter: TLS material is
  infrastructure/secrets-management, not something this task introduces.
