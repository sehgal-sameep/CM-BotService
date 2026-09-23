# Chat API Guide — How a Message Flows Through This Backend

A plain-English walkthrough of the one API this service exposes: what the frontend
sends us, what we forward to the ML Agent, and what the ML Agent streams back, which
this backend passes on to the frontend unchanged. No code required to read this — just JSON examples.

For the "why" behind design decisions, see [`ARCHITECTURE.md`](ARCHITECTURE.md). This
doc is the "what" — a reference you'd hand to a frontend developer or a new teammate.

## 1. The big picture

```text
┌──────────┐   1. one HTTP POST    ┌─────────────┐   2. one gRPC call   ┌───────────┐
│          │  ───────────────────▶ │             │ ───────────────────▶│           │
│ Frontend │                        │ This Backend│                      │ ML Agent  │
│ (chat UI)│  ◀─────────────────── │ (this repo) │ ◀───────────────────│ (Thoughtful│
│          │   4. SSE stream back   │             │   3. gRPC stream back│  Labs)    │
└──────────┘                        └─────────────┘                      └───────────┘
```

This backend never stores anything. It takes one message in, forwards it, and streams
the ML Agent's answer straight back out, event for event, in the ML Agent's own format.
It's a pass-through proxy, not a translator, a database, or a session manager. Every request is independent; **the only thing
"remembered" between requests is whatever the frontend chooses to resend as `history`**
(see §5, "continuing a conversation") — there is no token or id to save and echo back.

## 2. Key terms (read this before the rest)

| Term | What it means |
|---|---|
| `tenantId` | Which customer this request belongs to. Sent as the `X-Tenant-Id` request header, not a body field. |
| `organization` | Which organization within that tenant this request belongs to. Sent as the `X-Org-Id` request header. |
| `caseId` | Which fraud case the analyst is chatting about. |
| `history` | The full conversation transcript so far, oldest turn first. This is the **only** way to continue a conversation — the ML Agent's contract has no session/continuation token at all. Resend the growing transcript on every follow-up message. |
| `messageId` | A unique ID this backend generates per message, for tracing in logs. Not something the frontend sends; only appears in a `service_error` event (§6). |
| `requestId` | Optional, frontend-generated. Purely for your own tracing/support tickets — this backend logs it but doesn't use it for anything else. |
| `endUserId` | Optional hint about who the message is really about/for. Forwarded to the ML Agent's case context as-is; this backend never interprets or defaults it. |
| `operatorId` | The analyst's identity, taken from `X-User-Id` (or the BFF session, once auth is fully wired up) — not something the frontend sends as a body field. |

## 3. Step 1 — What the frontend sends to this backend

**One endpoint, one method:**

```
POST /api/v1/chat/messages
Content-Type: application/json
Accept: text/event-stream
```

**Required headers:**

| Header | Purpose |
|---|---|
| `X-Tenant-Id` | Which customer this request belongs to. Missing or blank → `400 VALIDATION_ERROR`, request never reaches the ML Agent. |
| `X-Org-Id` | Which organization within that tenant. Missing or blank → `400 VALIDATION_ERROR`, same as above. |

**Optional headers:**

| Header | Purpose |
|---|---|
| `X-User-Id` | The analyst's identity (stand-in until real login/auth exists) — becomes `operatorId` on the ML Agent request. |
| `X-Correlation-Id` | Your own tracing ID — if you don't send one, this backend generates one and echoes it back on the response header. |

**Request body:**

```json
{
  "caseId": "case-1001",
  "history": [],
  "requestId": "req-a1b2c3",
  "endUserId": null,
  "message": "Summarize this case for me"
}
```

| Field | Required? | Notes |
|---|---|---|
| `caseId` | **Yes** | Letters, digits, `_`, `-` only. Max 100 chars. |
| `history` | No | Omit (or send an empty array) to start a brand-new conversation. Otherwise, resend the full transcript so far — see §7. Each turn is `{ "role": "user"|"assistant", "content": "..." }`; at most 50 turns. |
| `requestId` | No | Your own tracking ID, for your logs only. |
| `endUserId` | No | Forwarded to the ML Agent as-is; not interpreted by this backend. |
| `message` | **Yes** | The analyst's question/prompt. Max 4000 characters. |

**That's it — this is the entire contract the frontend needs to know.** §4 happens
inside this backend and is invisible to the frontend. §5–§6 describe what you receive,
which is the ML Agent's own event format.

## 4. Step 2 — What this backend forwards to the ML Agent

This backend re-shapes your request into the ML Agent's own contract before
forwarding it, over gRPC (not another HTTP call) — shown here as JSON since that's the
simplest way to read it. You never see this directly, but it's useful to know the
mapping if something looks wrong end-to-end:

```json
{
  "requestContext": {
    "tenant": "tenant-42",
    "organization": "org-7",
    "agentSessionId": "req-a1b2c3",
    "requestId": "<this backend's own correlation id>"
  },
  "operatorId": "analyst-1",
  "prompt": "Summarize this case for me",
  "caseContext": {
    "caseId": "case-1001",
    "endUserId": null
  },
  "history": []
}
```

| Your field | Becomes | Notes |
|---|---|---|
| `X-Tenant-Id` (header) | `requestContext.tenant` | Passed straight through. |
| `X-Org-Id` (header) | `requestContext.organization` | Passed straight through — not looked up or validated against anything server-side. |
| `requestId` | `requestContext.agentSessionId` | Reused as the closest thing this backend has to a request-grouping id; blank if you didn't send one. |
| — | `requestContext.requestId` | This backend's own internal correlation id (from `X-Correlation-Id` or generated) — **not** your `requestId` field, despite the similar name. |
| `X-User-Id` (header) | `operatorId` | The analyst identity, not a body field. |
| `message` | `prompt` | Untouched. |
| `caseId` | `caseContext.caseId` | Nested. |
| `endUserId` | `caseContext.endUserId` | Nested. |
| `history` | `history` | Each `{role, content}` turn becomes a `user`/`agent` turn in the ML Agent's own shape — `role: "user"` maps to a user turn, anything else to an agent turn. |

Fields that **never** leave this backend: `X-Correlation-Id` (goes out as
`requestContext.requestId`, not literally the header value's name), `messageId`. Those
exist purely for this backend's own logging/tracing — `correlationId`/`tenantId`/
`caseId` are already enough to trace one chatbot interaction end to end.

## 5. Step 3 — What the ML Agent streams back

The ML Agent responds with a live stream of `AnswerEvent` messages over gRPC
(`src/main/proto/chat_agent.proto`). Each one has exactly one of seven event types set:

| Event | Meaning | How many? |
|---|---|---|
| `chunk` | One small piece of the answer text, arriving live as it's generated | Many |
| `tool_call` | The agent invoked a tool (a DB query, a rules check, etc.) | 0 or more, any point in the stream |
| `tool_result` | The result of that tool invocation (`tool_call_id` matches its `tool_call`) | One per `tool_call` |
| `ping` | A pure keepalive, no content | 0 or more |
| `payload` | The structured, cited analysis for the UI to render | May arrive at any point |
| `done` | "I'm finished" — terminal, the answer is valid | One, last, on success |
| `error` | A model-level failure — terminal, no usable answer | Instead of `done` |

The ML Agent's own contract rules (from the `.proto` comments) apply as-is, because
this backend passes them straight through: consecutive `chunk` events form one text
block, any other event closes it; clients must treat an unrecognised `tool_result.status`
as `STATUS_FAILED` and an unrecognised `error.code` as `ERROR_CODE_INTERNAL`; clients
must ignore event types they don't recognise; after an `error`, discard the chunks
already received and don't persist the turn.

## 6. Step 4 — What this backend streams back to the frontend

**Exactly what the ML Agent sent, event for event.** This backend doesn't rename
events or fields, reshape payloads, drop fields, or add its own wrapper. Each
`AnswerEvent` becomes one SSE event:

| SSE line | Value |
|---|---|
| `event:` | The ML Agent's own event type name — `chunk`, `tool_call`, `tool_result`, `payload`, `done`, `error`, `ping` |
| `data:` | The whole `AnswerEvent`, as standard protobuf JSON ([proto3 JSON mapping](https://protobuf.dev/programming-guides/json/)) with the **original `.proto` field names** (`tool_call_id`, `args_json`, `stop_reason`, …) and the original nesting |
| `id:` | A frame counter (0, 1, 2, …) — the only thing this backend adds, and only as SSE transport metadata |

What that JSON looks like for each event type:

| `event:` | `data:` |
|---|---|
| `chunk` | `{"chunk":{"delta":"This case was..."}}` |
| `tool_call` | `{"tool_call":{"tool_call_id":"t-1","name":"getCase","args_json":"{\"caseId\":\"case-1001\"}"}}` |
| `tool_result` | `{"tool_result":{"tool_call_id":"t-1","status":"STATUS_OK","ms":"42","row_count":"7"}}` |
| `payload` | `{"payload":{"case_manager_answer_payload":{"key_signals":[{"signal":"Unusual device/IP","citations":["evt-123"]}],"citations":[{"id":"evt-123","source":"getCase","fields":["risk_score"]}]}}}` |
| `done` | `{"done":{"stop_reason":"STOP_REASON_COMPLETED","latency_ms":"1800","tokens_in":"12","tokens_out":"140"}}` |
| `error` | `{"error":{"code":"ERROR_CODE_MODEL_REFUSED","retryable":false}}` |
| `ping` | `{"ping":{}}` |

Three things about that JSON format that frontend code needs to handle:

- **`int64` numbers come through as JSON strings** — `"ms":"42"`, `"row_count":"7"`,
  `"latency_ms":"1800"`, `"tokens_in"`, `"tokens_out"`. That's the standard proto3 JSON
  rule, since JavaScript numbers can't hold every `int64`. Use `Number(...)` to convert.
- **Enums are their full proto names** — `STATUS_OK`/`STATUS_FAILED`/`STATUS_UNSPECIFIED`,
  `STOP_REASON_COMPLETED`/`STOP_REASON_TRUNCATED`, `ERROR_CODE_MODEL_REFUSED`/
  `ERROR_CODE_DATA_UNAVAILABLE`/`ERROR_CODE_INTERNAL`.
- **Fields at their default value are still included** (`"ms":"0"`, `"retryable":false`,
  `"tool_call_id":""`), so no field is ever silently missing. The exception is
  `row_count`: it belongs to a `oneof` (`result_summary`), so it only appears when the
  agent actually set it. A `tool_result` without `row_count` means the agent didn't
  report one.

`args_json` is itself a JSON string — the tool's arguments exactly as the agent
serialized them. Parse it separately if you need it.

The only event the ML Agent sends that you won't see is one with **no** event type set
(an empty message, or a type newer than this backend's copy of the `.proto`). The
contract says to ignore those, and there's nothing in them to forward.

### The one event this backend adds: `service_error`

Some failures never produce an ML Agent event at all: the agent can't be reached, it
times out, it rejects the call at the gRPC level, or this backend's own protection
(circuit breaker, concurrency limit, message-length check) stops the request. The HTTP
status is already committed to `200` by then, so this backend sends one terminal event
of its own, under a name the ML Agent never uses:

```
event:service_error
data:{"messageId":"5b1e...","errorCode":"ML_AGENT_TIMEOUT","errorMessage":"The AI agent did not respond in time.","timestamp":"2026-09-23T10:15:30Z"}
```

This is **not** the ML Agent's `error` event, and the agent's `error` event is never
converted into it. Handle the two separately (see §8).

Every stream ends with exactly one of `done`, `error`, or `service_error`.

## 7. Full example: a two-turn conversation

**Turn 1 — analyst opens a new chat:**

```bash
curl -N -X POST http://localhost:8080/api/v1/chat/messages \
  -H "Content-Type: application/json" -H "Accept: text/event-stream" \
  -H "X-Tenant-Id: tenant-42" -H "X-Org-Id: org-7" \
  -d '{"caseId":"case-1001","message":"Summarize this case for me"}'
```

```
id:0
event:tool_call
data:{"tool_call":{"tool_call_id":"t-1","name":"getCase","args_json":"{\"caseId\":\"case-1001\"}"}}

id:1
event:tool_result
data:{"tool_result":{"tool_call_id":"t-1","status":"STATUS_OK","ms":"42","row_count":"7"}}

id:2
event:chunk
data:{"chunk":{"delta":"This case was created because..."}}

... more "chunk" events (and possibly "ping") ...

id:7
event:payload
data:{"payload":{"case_manager_answer_payload":{"key_signals":[...],"citations":[...]}}}

id:8
event:done
data:{"done":{"stop_reason":"STOP_REASON_COMPLETED","latency_ms":"1800","tokens_in":"12","tokens_out":"140"}}
```

In the browser (`EventSource` only supports GET, so use a fetch-based SSE reader such
as `@microsoft/fetch-event-source` for this POST endpoint), branch on the event name and
read the agent's own fields:

```js
onmessage(msg) {
  const data = JSON.parse(msg.data);
  switch (msg.event) {
    case "chunk":         appendText(data.chunk.delta); break;
    case "tool_call":     showTool(data.tool_call.tool_call_id, data.tool_call.name); break;
    case "tool_result":   finishTool(data.tool_result.tool_call_id, data.tool_result.status); break;
    case "payload":       renderSignals(data.payload.case_manager_answer_payload); break;
    case "done":          finish(data.done.stop_reason === "STOP_REASON_TRUNCATED"); break;
    case "error":         failModel(data.error.code, data.error.retryable); break;
    case "service_error": failService(data.errorCode, data.errorMessage); break;
    default:              /* ping, or a newer event type — ignore */ break;
  }
}
```

The frontend appends both this turn's prompt and the assembled answer text (from the
`chunk` events) to its own local transcript — this backend never remembers any of it
for you.

**Turn 2 — follow-up question, same conversation:**

```bash
curl -N -X POST http://localhost:8080/api/v1/chat/messages \
  -H "Content-Type: application/json" -H "Accept: text/event-stream" \
  -H "X-Tenant-Id: tenant-42" -H "X-Org-Id: org-7" \
  -d '{"caseId":"case-1001","history":[{"role":"user","content":"Summarize this case for me"},{"role":"assistant","content":"This case was..."}],"message":"Which rules were triggered?"}'
```

Same event sequence as Turn 1 — there's no id to echo back or compare; `history`
carrying the prior turn is what makes this a continuation rather than a fresh
conversation.

## 8. When things go wrong

Once streaming has started, this backend never returns an HTTP error status, so the
frontend should always watch for the two terminal failure events rather than expecting
an HTTP status code mid-stream. (A request that's invalid up front, such as a missing
`X-Tenant-Id` or `caseId`, is still rejected with a plain HTTP `400`, before any SSE.)

**`error` — the ML Agent's own model-level failure**, forwarded exactly as sent:

| `error.code` | Meaning (per the ML Agent contract) |
|---|---|
| `ERROR_CODE_MODEL_REFUSED` | The agent refused to process the request. Never retryable. |
| `ERROR_CODE_DATA_UNAVAILABLE` | The agent's data source was unavailable. |
| `ERROR_CODE_INTERNAL` | An internal error inside the agent. Also how to treat any unrecognised code. |
| `ERROR_CODE_UNSPECIFIED` | No code given — treat as `ERROR_CODE_INTERNAL`. |

`error.retryable` is the agent's own answer to "should the caller retry?" — use it
directly.

**`service_error` — a failure on this backend's side or in reaching the agent:**

| `errorCode` | What happened | Should the frontend retry? |
|---|---|---|
| `VALIDATION_ERROR` | The message exceeded the runtime length limit | No — fix the request |
| `NOT_FOUND` | The agent rejected the call: case not found for that tenant | No |
| `ML_AGENT_TIMEOUT` | The ML Agent took too long to respond | Maybe, after a pause |
| `ML_AGENT_UNAVAILABLE` | This backend couldn't reach the ML Agent at all | Maybe, after a pause |
| `ML_AGENT_ERROR` | The ML Agent call failed at the transport level | Maybe, after a pause |
| `CONCURRENCY_LIMIT_REACHED` | This backend is protecting itself/the ML Agent from overload right now | Yes, after a short pause |
| `INTERNAL_ERROR` | Something unexpected on this backend's side, or a credential/access problem between this backend and the ML Agent | No — this is not something the frontend can fix |

`errorMessage` is always a short, safe, human-readable sentence — never a raw
exception or stack trace. Transient failures that happen *before* the ML Agent sends
its first event are already retried by this backend (a few times, with backoff) before
a `service_error` is sent.

## 9. Testing this without a real ML Agent

By default this backend runs against a **mock** ML Agent (no real agent needed —
great for frontend development). The mock emits the real contract's own messages, so
what you receive is exactly the shape the real agent will produce. Trigger specific
behaviors by including these keywords anywhere in your `message` text:

| Keyword | Simulates |
|---|---|
| `trigger:slow` | A normal response, but much slower between chunks |
| `trigger:timeout` | The agent never responds → `service_error` `ML_AGENT_TIMEOUT` |
| `trigger:error` | The agent can't be reached/fails at the transport level → `service_error` `ML_AGENT_ERROR` (after this backend's own retries) |
| `trigger:agent-error` | The agent's own `error` event → `error` `{"code":"ERROR_CODE_DATA_UNAVAILABLE","retryable":true}` |
| `trigger:empty` | A valid but empty response: just `done` (no `chunk`/`payload`) |
| `trigger:rejected` | The agent rejects the request → `service_error` `NOT_FOUND` |

Any other text gets a canned, realistic-looking fraud-case answer: a
`tool_call`/`tool_result` pair, `chunk`s, a `payload` with citations, then `done`. That
makes the full contract (§5–§6) testable end-to-end without waiting on the real ML
Agent integration.

## 10. Quick-reference field map

```text
FRONTEND SENDS               THIS BACKEND FORWARDS              ML AGENT RETURNS  ═══▶  FRONTEND RECEIVES (unchanged)
─────────────────            ────────────────────────────       ─────────────────       ─────────────────────────────
X-Tenant-Id (header)  ───▶  requestContext.tenant               chunk              ═══▶  event:chunk        {"chunk":{...}}
X-Org-Id (header)     ───▶  requestContext.organization         tool_call          ═══▶  event:tool_call    {"tool_call":{...}}
caseId                ───▶  caseContext.caseId                  tool_result        ═══▶  event:tool_result  {"tool_result":{...}}
endUserId             ───▶  caseContext.endUserId               payload            ═══▶  event:payload      {"payload":{...}}
history               ───▶  history (role/content ─▶ user/agent) done              ═══▶  event:done         {"done":{...}}
message               ───▶  prompt                              error              ═══▶  event:error        {"error":{...}}
X-User-Id             ───▶  operatorId                          ping               ═══▶  event:ping         {"ping":{}}
requestId             ───▶  requestContext.agentSessionId
X-Correlation-Id      ───▶  requestContext.requestId            (no agent event)   ───▶  event:service_error (this backend's own)
```

The request side (left) is still mapped into the ML Agent's gRPC request shape. The
response side (right) is not mapped at all.
