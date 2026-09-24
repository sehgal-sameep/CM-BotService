# API curl Reference

Ready-to-run curl commands for every endpoint this service exposes. Use these instead
of Swagger UI's "Try it out" for real requests. Swagger UI
(`http://localhost:8080/swagger-ui.html`) is still the place to *read* the API contract,
but its "Try it out" can't do two things this API needs:

- **Send a session cookie.** Browsers forbid page JavaScript from setting the `Cookie`
  header, so any `BFF_SESSION`-mode request from Swagger arrives with no session (see
  [`SESSION_DEBUG_API.md`](SESSION_DEBUG_API.md)).
- **Show a live SSE stream.** Swagger waits for the connection to close and then shows
  the buffered body, so you never see events arrive one by one.

Every example below uses placeholders in `<angle brackets>`. Replace them before
running.

## Before you start

**Base URL:** `http://localhost:8080` (or whatever `SERVER_PORT` is set to).

**Which modes you're running in** decides which commands apply. The startup log tells
you:

| Startup log line | Meaning |
|---|---|
| `ML_AGENT_MOCK_CONFIGURED` | `ml-agent.mode=mock`: the built-in mock answers, and the `trigger:*` keywords work |
| `ML_AGENT_GRPC_CHANNEL_CONFIGURED target=<host>:<port>` | `ml-agent.mode=grpc`: requests go to the real TF Labs orchestrator |
| the `chatbot.security.mode = NONE` banner | no authentication; send `X-User-Id` instead of a cookie |
| `REDIS_SESSION_STORE_CONFIGURED endpoint=<host>:<port> (...)` | `chatbot.security.mode=BFF_SESSION`: the chat endpoint needs a `SESSION` cookie, and the debug endpoint exists |

**Shell syntax.** The examples use Git Bash / macOS / Linux syntax (`\` line
continuation, single-quoted JSON). On Windows:

- **PowerShell:** type `curl.exe`, not `curl`. In Windows PowerShell 5.1, `curl` is an
  alias for `Invoke-WebRequest`. Use a backtick `` ` `` for line continuation, and put the
  JSON body in a file so you don't have to fight PowerShell quoting:
  ```powershell
  '{"caseId":"case-456","message":"Summarize this case for me"}' | Out-File -Encoding ascii body.json
  curl.exe -N -X POST "http://localhost:8080/api/v1/chat/messages" `
    -H "Content-Type: application/json" -H "Accept: text/event-stream" `
    -H "X-Tenant-Id: tenant-123" -H "X-Org-Id: org-123" `
    --data-binary "@body.json"
  ```
- **cmd.exe:** one line (or `^` continuation), and escape inner double quotes:
  `-d "{\"caseId\":\"case-456\",\"message\":\"hi\"}"`.

**Useful curl flags:**

| Flag | Why |
|---|---|
| `-N` | Don't buffer output; SSE events print as they arrive. Always use it for chat. |
| `-i` | Show the response status and headers (including the echoed `X-Correlation-Id`). |
| `-H "X-Correlation-Id: <your-id>"` | Tag the request so you can `grep` its whole log trail (see [Finding a request in the logs](#finding-a-request-in-the-logs)). |

---

## 1. Chat — `POST /api/v1/chat/messages`

Streams the ML Agent's answer back as SSE, forwarded unchanged from the TF Labs
orchestrator. The event format is in [`CHAT_API_GUIDE.md`](CHAT_API_GUIDE.md) §6.

**Required headers:** `X-Tenant-Id`, `X-Org-Id`, `Content-Type: application/json`.
**Required body fields:** `caseId`, `message`.

### 1.1 Security mode `NONE` (local development)

New conversation:

```bash
curl -N -i -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "X-Correlation-Id: <your-id>" \
  -H "X-User-Id: <analyst-id>" \
  -H "X-Tenant-Id: <tenant>" \
  -H "X-Org-Id: <org>" \
  -d '{"caseId":"<case-id>","message":"Summarize this case for me"}'
```

Continue the conversation: resend the transcript so far as `history`, oldest turn
first:

```bash
curl -N -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "X-User-Id: <analyst-id>" \
  -H "X-Tenant-Id: <tenant>" \
  -H "X-Org-Id: <org>" \
  -d '{
        "caseId": "<case-id>",
        "history": [
          {"role": "user", "content": "Summarize this case for me"},
          {"role": "assistant", "content": "<the answer text you received>"}
        ],
        "message": "Which rules were triggered?"
      }'
```

All optional body fields:

```bash
  -d '{"caseId":"<case-id>","history":[],"requestId":"<your-request-id>","endUserId":"<end-user-id>","message":"<prompt>"}'
```

### 1.2 Security mode `BFF_SESSION`

Same request, but identity comes from the BFF session cookie, so drop `X-User-Id` and
add the cookie. `X-Tenant-Id` must be the tenant the session belongs to, because it's
part of the Redis key.

```bash
curl -N -i -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" \
  -H "Accept: text/event-stream" \
  -H "X-Correlation-Id: <your-id>" \
  -H "Cookie: SESSION=<session value>" \
  -H "X-Tenant-Id: <tenant>" \
  -H "X-Org-Id: <org>" \
  -d '{"caseId":"<case-id>","message":"Summarize this case for me"}'
```

`<session value>` is only the cookie's **value**, copied from the FMC UI (DevTools →
Application → Cookies → `SESSION`). To check the session on its own first, use the
debug endpoint in §2.

Auth failures come back as plain JSON before any stream starts:

| Response | Meaning |
|---|---|
| `401 UNAUTHENTICATED` | No `SESSION` cookie, no `X-Tenant-Id`, or no Redis record for that session + tenant |
| `503 SESSION_STORE_UNAVAILABLE` | Redis is unreachable. The `REDIS_SESSION_LOOKUP_FAILED` log line has the endpoint and root cause. |

### 1.3 Mock-mode scenarios (`ml-agent.mode=mock` only)

Put a keyword anywhere in `message` to simulate each outcome:

```bash
BASE="http://localhost:8080/api/v1/chat/messages"
H=(-H "Content-Type: application/json" -H "Accept: text/event-stream" -H "X-Tenant-Id: t" -H "X-Org-Id: o")

curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"hello"}'                 # normal answer
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:slow"}'          # slow chunks
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:empty"}'         # just `done`
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:agent-error"}'   # agent's own `error` event
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:error"}'         # retried, then `service_error` ML_AGENT_ERROR
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:rejected"}'      # `service_error` NOT_FOUND
curl -N -X POST "$BASE" "${H[@]}" -d '{"caseId":"c","message":"trigger:timeout"}'       # ~5s, then `service_error` ML_AGENT_TIMEOUT
```

(The `H=(...)` array is Bash syntax. In other shells, repeat the headers on each line.)

### 1.4 Validation errors (plain `400` JSON, no stream)

```bash
# missing caseId -> 400 VALIDATION_ERROR "caseId: caseId must not be blank"
curl -s -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" -H "X-Tenant-Id: t" -H "X-Org-Id: o" \
  -d '{"message":"hi"}'

# missing X-Tenant-Id -> 400 VALIDATION_ERROR "X-Tenant-Id header must not be blank"
curl -s -X POST "http://localhost:8080/api/v1/chat/messages" \
  -H "Content-Type: application/json" -H "X-Org-Id: o" \
  -d '{"caseId":"c","message":"hi"}'
```

### Reading the output

Each SSE event prints as `id:` / `event:` / `data:` lines followed by a blank line:

```
id:0
event:tool_call
data:{"tool_call":{"tool_call_id":"t-1","name":"getCase","args_json":"{\"caseId\":\"case-456\"}"}}

id:1
event:chunk
data:{"chunk":{"delta":"This case was created because..."}}
```

The stream ends with exactly one of `done`, `error` (from the ML Agent), or
`service_error` (from this backend).

---

## 2. Session lookup (temporary) — `GET /api/v1/debug/session-lookup`

Only exists in `chatbot.security.mode=BFF_SESSION`. It checks whether a session cookie
and tenant resolve to a Redis record, without sending a chat message. Full details and
troubleshooting are in [`SESSION_DEBUG_API.md`](SESSION_DEBUG_API.md).

```bash
curl -i "http://localhost:8080/api/v1/debug/session-lookup" \
  -H "X-Correlation-Id: <your-id>" \
  -H "Cookie: SESSION=<session value>" \
  -H "X-Tenant-Id: <tenant>"
```

PowerShell:

```powershell
curl.exe -i "http://localhost:8080/api/v1/debug/session-lookup" `
  -H "Cookie: SESSION=<session value>" `
  -H "X-Tenant-Id: <tenant>"
```

`200` = record found (the body contains live tokens, so don't share it), `401` = no
record for that pair, `503` = Redis unreachable, `404` = not running in `BFF_SESSION`
mode.

---

## 3. Operational endpoints

No authentication in either security mode:

```bash
curl -s http://localhost:8080/actuator/health                 # overall health
curl -s http://localhost:8080/actuator/health/liveness        # k8s liveness
curl -s http://localhost:8080/actuator/health/readiness       # k8s readiness (stays UP while the ML Agent breaker is open)
curl -s http://localhost:8080/actuator/circuitbreakers        # ML Agent circuit breaker state
curl -s http://localhost:8080/actuator/prometheus | grep -E "^(chat|ml|sse)_"   # this service's metrics
curl -s http://localhost:8080/v3/api-docs                     # OpenAPI document (JSON)
```

---

## Finding a request in the logs

Send your own `-H "X-Correlation-Id: <your-id>"` (or read the one echoed in the
response headers with `-i`), then filter the logs by it. Every line of that request
carries `corrId=<your-id>`:

```bash
grep "corrId=<your-id>" app.log
```

A normal chat request produces, in order:

```
HTTP_REQUEST_RECEIVED        method, path, which headers/cookie are present
AUTH_CHECK_STARTED           (BFF_SESSION only) masked session, tenant
REDIS_SESSION_LOOKUP_STARTED (BFF_SESSION only) masked key, Redis endpoint
REDIS_SESSION_RECORD_FOUND / _PARSED, AUTH_SUCCEEDED
REQUEST_CONTEXT_RESOLVED     tenant, org, case, user
CHAT_REQUEST_RECEIVED        messageId, message length, history size (never the text)
GRPC_CALL_STARTED            (grpc mode) target host:port   | MOCK_ML_AGENT_CALL_STARTED (mock mode)
ML_REQUEST_STARTED           timeouts in effect
ML_STREAM_STARTED            first event type + latency
ML_EVENT_TOOL_CALL / _TOOL_RESULT / _PAYLOAD / _DONE / _ERROR
ML_STREAM_COMPLETED          per-event-type counts (chunks and pings are counted here)
SSE_STREAM_COMPLETED         frames sent to the client
HTTP_REQUEST_COMPLETED       status, total duration
```

Failures appear in place of the remaining steps, at WARN or ERROR:
`AUTH_REJECTED`, `REDIS_SESSION_RECORD_NOT_FOUND`, `REDIS_SESSION_RECORD_INCOMPLETE`,
`REDIS_SESSION_LOOKUP_FAILED` (with endpoint and full cause chain), `GRPC_CALL_FAILED`
(with the gRPC status and description), `ML_REQUEST_RETRY`, `ML_REQUEST_TIMEOUT`,
`CIRCUIT_BREAKER_OPEN`, `CONCURRENCY_LIMIT_REACHED`, `ML_REQUEST_FAILED`,
`SSE_SERVICE_ERROR_SENT`, `REQUEST_VALIDATION_FAILED`.

Session IDs appear only masked (`****cdef(len=29)`). Tokens, prompts, history, and
answer text are never logged.
