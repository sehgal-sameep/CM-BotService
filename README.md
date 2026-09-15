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
(see `src/main/proto/chat_agent.proto`) — the frontend-facing contract above it stays
plain HTTP + SSE either way, since the two are entirely decoupled by this interface.

The full architecture rationale (why WebFlux, timeout/retry/circuit-breaker/bulkhead
design, SSE contract, tracing/MDC mechanism, etc.) is in
[`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — start there for the "why". For a
simple, non-code walkthrough of the request/response flow and payloads (frontend ↔
this backend ↔ ML Agent), see
[`docs/CHAT_API_GUIDE.md`](docs/CHAT_API_GUIDE.md) — hand that one to a frontend
developer or a new teammate.

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
`context.endUserId` — the contract documents this as a SHA-256 hash, base64-encoded;
this backend does not compute that hash, it only forwards whatever value it's given
(see "Known limitations").

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

## Authentication

`chatbot.security.mode` is the master toggle, checked before the request ever reaches
`ChatController`:

- **`NONE`** (default here, local-dev only) — bypasses all authentication; every
  request is permitted unchanged. Logs a loud `WARN` banner on every startup while
  active, so it's impossible to miss in logs if accidentally left on.
- **`BFF_SESSION`** (the only mode for any shared/prod deployment) — validates an
  existing BFF-issued session, read-only, against the same Redis instance FMC-PM-BFF
  uses: extract the session cookie → look up the record (never write/refresh/delete)
  → check access-token expiry → validate CSRF (cookie vs. header) → cross-check an
  optional tenant header against the session's tenant → filter the session's
  permissions to the `CHATBOT_`-prefixed subset and reject if none remain. Every
  rejection returns the same `ErrorResponse` shape used elsewhere in this API
  (`401 UNAUTHENTICATED`, `403 FORBIDDEN`, or `503 SESSION_STORE_UNAVAILABLE` if Redis
  itself is unreachable).

Only `POST /api/v1/chat/messages` is actually gated — actuator health/readiness,
Swagger UI, and the OpenAPI JSON stay reachable without a session either way, since
a k8s liveness/readiness prober has no BFF session cookie to send.

```bash
# mode: NONE (default) — works with no cookie at all
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" -H "Content-Type: application/json" -d '{"tenantId":"t","caseId":"c","message":"hello"}'

# mode: BFF_SESSION — needs a valid session + matching CSRF cookie/header
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Cookie: SESSION=<value>; XSRF-TOKEN=<csrf-value>" \
  -H "X-XSRF-TOKEN: <csrf-value>" \
  -d '{"tenantId":"t","caseId":"c","message":"hello"}'
```

| Property | Purpose |
|---|---|
| `chatbot.security.mode` | `NONE` (default) or `BFF_SESSION` — the master toggle |
| `chatbot.security.session.cookie-name` | The BFF session cookie's name (default `SESSION`) |
| `chatbot.security.session.tenant-header-name` | Optional tenant cross-check header name — **no default is documented in the source design; `X-Tenant-Id` is this service's own placeholder, unconfirmed** |
| `chatbot.security.redis.*` | Host/port/ssl/password/namespace for the shared, read-only Redis connection, plus `strategy` (selects the `SessionStore` bean) and `field-names.*` (maps the assumed session JSON's field names — see "Known limitations") |
| `chatbot.security.csrf.*` | `enabled`, `cookie-name` (default `XSRF-TOKEN`), `header-name` (default `X-XSRF-TOKEN`) |
| `chatbot.security.authorization.*` | `required-permission-prefix` (default `CHATBOT_`), `permit-when-no-chatbot-permissions` (default `false`) |
| `chatbot.security.cors.allowed-origins` | Explicit FMC UI origins allowed with credentials — no wildcard, ever |
| `chatbot.security.fail-open-on-redis-error` | Insecure local-dev-only escape hatch (default `false`): permit the request through, unauthenticated, if Redis is unreachable instead of rejecting with 503 |

See `docs/ARCHITECTURE.md`'s Authentication section for the full flow rationale and
every judgment call this implementation made against an intentionally
not-fully-specified design.

## SSE event contract (FE ↔ BE)

| Event | Payload | Cardinality |
|---|---|---|
| `stream-start` | `{ conversationId, messageId, timestamp }` | one, first — `conversationId` here is an **unconfirmed echo** of the request, not authoritative |
| `message` | `{ conversationId, messageId, sequence, content, timestamp }` | many — one streamed answer fragment (the ML Agent's own `token`/`delta`) |
| `payload` | `{ conversationId, messageId, payload, timestamp }` | exactly one, always before `stream-complete` — the structured, cited case analysis (`answer`, `summary.narrative/keySignals/entities/timeline`, `suggestedResolution`, `citations`) |
| `stream-complete` | `{ conversationId, continuation, messageId, totalChunks, timestamp }` | one, terminal — **the only authoritative source** of `conversationId`/`continuation`; echo both back on the next message |
| `error` | `{ conversationId, messageId, errorCode, message, timestamp }` | terminal |

The ML Agent's own `tool_call`/`tool_result` events (its internal tool-orchestration
trace) are consumed and logged (DEBUG) by `GrpcMlAgentClient`/`MockMlAgentClient` and
**never** forwarded as an SSE event — this backend is the product, not the sandbox
the ML Agent's own docs describe those events as being rendered in.

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
| `ml-agent.mode` | `mock` (default) or `grpc` — selects the active `MlAgentClient` bean |
| `ml-agent.grpc-host` / `grpc-port` | Only used in `mode: grpc` — the ML Agent's gRPC endpoint |
| `ml-agent.first-response-timeout` / `idle-timeout` | Per-element `Flux` timeout, client-agnostic (no separate gRPC deadline exists) |
| `ml-agent.grpc-max-inbound-message-size` | Max size of one message the gRPC channel will accept (`mode: grpc` only) |
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
- `GrpcMlAgentClientTest` — in-process gRPC server (the gRPC analog of MockWebServer):
  real request field mapping assertion (matches the ML Agent's proto contract), all
  six event types including `tool_call`/`tool_result` being consumed silently, both
  `payload` invariant validations, an unset-oneof malformed case, gRPC `Status.Code` →
  exception mapping, and in-stream `error` event code mapping (incl. 4221/4222 →
  continuation expired) — same coverage the old MockWebServer-based test had, just
  against the new transport.
- `ChatControllerTest` — full stack (`RestTestClient` against a real random port):
  SSE ordering (incl. the new `payload` event), validation, ML-failure-as-error-event,
  correlation ID echo, `continuation`/`conversationId` round-tripping across two
  requests, 404.

## Replacing the mock with the real ML Agent

Set `ml-agent.mode: grpc` and provide the real `ml-agent.grpc-host`/`grpc-port` —
`GrpcMlAgentClient` activates and `MockMlAgentClient` steps aside automatically
(`@ConditionalOnProperty` on both). No controller or `ChatOrchestrationService` change
required — this is the payoff of `MlAgentClient` being a real seam: the transport
underneath it (HTTP+SSE, then gRPC) has changed twice now without either of those
classes noticing. `GrpcMlAgentClient` speaks the ML Agent's `Chat` RPC
(`src/main/proto/chat_agent.proto`, a straight protobuf translation of the originally
documented `POST /v1/chat` contract) — see the SSE event contract table above and
`docs/ARCHITECTURE.md §2` for the full request/response shape, including the two
payload invariants it validates (every key signal needs a citation;
`suggestedResolution.mark` must be a known code).

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

- **The Redis session layout `JsonBlobSessionStore` assumes is unconfirmed.** The
  documented flow explicitly states the FMC-PM-BFF key format/serialization needs
  confirming and to implement behind a swappable strategy in the meantime — that's
  exactly what `SessionStore`/`chatbot.security.redis.strategy` is. The current
  default assumes one JSON document per session at a plain string key; if the real
  layout is structurally different (e.g. Spring Session's per-attribute hash scheme),
  a new `SessionStore` implementation is needed, not a config change.
- `chatbot.security.session.tenant-header-name` has no documented default in the
  source design (unlike the cookie/CSRF property names, which do) — `X-Tenant-Id` is
  this service's own placeholder pick, unconfirmed.
- The documented flow describes "endpoint-level access enforced by required
  permission (e.g., write vs. read)" as a general mechanism, but names no concrete
  required permission for this service's one real endpoint. Only the confirmed part
  is implemented: sessions are filtered to their `CHATBOT_`-prefixed permissions and
  rejected if none remain. The granted-authority list is exposed on the exchange for
  a future per-endpoint check, but no such check exists today.
- No client-side hashing exists for `endUserId`'s documented SHA-256 format (see the
  ML Agent contract note above) — unrelated to authentication, but the same
  "documented format, unconfirmed responsibility" pattern.
- The real contract's `contextToken`/product-auth path isn't wired up — this backend
  currently always sends the sandbox-path `tenantId`, since there's no real auth/JWT
  infrastructure yet. Documented gap, not a bug; see `docs/ARCHITECTURE.md §2`.
- `context.endUserId` is documented as a SHA-256 hash, base64-encoded — but **who is
  responsible for computing that hash is not specified anywhere in the contract**.
  This backend forwards whatever `ChatRequest.endUserId` supplies verbatim (`null` if
  omitted) and does not hash it. If the real contract expects this backend to hash a
  raw value, that's unimplemented; flagged here rather than guessed at.
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
- Individual ML response chunk size isn't independently bounded beyond the gRPC
  channel's overall `grpc-max-inbound-message-size` — a concrete per-chunk limit would
  need the real ML Agent's contract to be meaningful.
- No TLS is configured on the gRPC channel (`GrpcChannelConfig` uses plaintext) —
  deliberately out of scope, same as the tracing exporter: TLS material is
  infrastructure/secrets-management, not something this task introduces.
