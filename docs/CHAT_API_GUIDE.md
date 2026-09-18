# Chat API Guide — How a Message Flows Through This Backend

A plain-English walkthrough of the one API this service exposes: what the frontend
sends us, what we forward to the ML Agent, what the ML Agent streams back, and what we
stream back to the frontend. No code required to read this — just JSON examples.

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
the answer straight back out — a translator sitting between two different contracts,
not a database or a session manager. Every request is independent; **the only thing
"remembered" between requests is whatever the frontend chooses to resend as `history`**
(see §5, "continuing a conversation") — there is no token or id to save and echo back.

## 2. Key terms (read this before the rest)

| Term | What it means |
|---|---|
| `tenantId` | Which customer this request belongs to. Sent as the `X-Tenant-Id` request header, not a body field. |
| `organization` | Which organization within that tenant this request belongs to. Sent as the `X-Org-Id` request header. |
| `caseId` | Which fraud case the analyst is chatting about. |
| `history` | The full conversation transcript so far, oldest turn first. This is the **only** way to continue a conversation — the ML Agent's contract has no session/continuation token at all. Resend the growing transcript on every follow-up message. |
| `messageId` | A unique ID this backend generates per message, for tracing in logs. Not something the frontend sends. |
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

**That's it — this is the entire contract the frontend needs to know.** Everything
past this point (§4–§6) happens inside this backend and is invisible to the frontend;
skip to §7 if you only care about what you send/receive.

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

The ML Agent responds with a live stream of events over gRPC. Seven possible event
types, always in roughly this order:

| Event | Meaning | How many? |
|---|---|---|
| `chunk` | One small piece of the answer text, arriving live as it's generated | Many |
| `tool_call` | The agent looked something up internally (a DB query, a rules check, etc.) | 0 or more — **forwarded to the frontend** as `tool-call` (see §6) |
| `tool_result` | The result of that lookup | One per `tool_call` — **forwarded to the frontend** as `tool-result` |
| `ping` | A pure keepalive, no content | 0 or more — swallowed, just logged; never forwarded |
| `payload` | The final, structured, cited analysis (see below) | At most one, before the stream ends |
| `done` | "I'm finished" | One, always last on success |
| `error` | "Something went wrong" | Only appears instead of `done`, never alongside it |

**The `tool_call`/`tool_result` events** — the agent's own trace of tool use, now
passed through to the frontend so the case manager can see what the agent did to
produce its answer:

```json
{ "toolCallId": "t-1", "name": "lookup_case_events", "argsJson": "{\"caseId\":\"case-1001\"}" }
```

```json
{ "toolCallId": "t-1", "status": "OK", "ms": 42, "rowCount": 7 }
```

`status` is `OK` or `FAILED` (any value the ML Agent doesn't recognize is treated as
`FAILED`, per its own contract). `rowCount` is only present when `status` is `OK` and
the ML Agent reported one — `null` otherwise. There's no guaranteed 1:1 timing with
`chunk` events; a `tool_call`/`tool_result` pair can arrive at any point in the stream.

**The `payload` event** is the important one — it's the structured answer for the
case manager UI to render. This is deliberately small: it's an overlay on top of the
answer text (which arrives via `chunk` events), not a duplicate of it — there's no
`answer`/`narrative`/`entities`/`timeline`/`suggestedResolution` here, just the
signals and their supporting citations:

```json
{
  "keySignals": [
    { "signal": "Unusual device/IP", "citations": ["evt-123"] }
  ],
  "citations": [
    { "id": "evt-123", "source": "AgenticGetCase.events[].decision", "fields": ["risk_score"] }
  ]
}
```

One rule this backend enforces on every `payload` it receives (and rejects the
response if it's broken): every `keySignal` must point to at least one entry in
`citations`.

**The `done` event** — carries usage/latency figures, plus whether the answer was cut
short:

```json
{ "stopReason": "COMPLETED", "latencyMs": 1800, "tokensIn": 12, "tokensOut": 140 }
```

There is **no** conversation/continuation token anywhere in this contract — resending
`history` is the only resumption mechanism (see §1, §7).

## 6. Step 4 — What this backend streams back to the frontend

This backend re-shapes the ML Agent's events into its own, simpler frontend-facing
contract — same events, cleaner field names. The internal `ping` keepalive is still
removed entirely, but `tool_call`/`tool_result` are now forwarded as `tool-call`/
`tool-result`:

| Event | Payload | Notes |
|---|---|---|
| `stream-start` | `{ messageId, timestamp }` | Sent immediately, once the ML Agent accepts the request. |
| `message` | `{ messageId, sequence, content, timestamp }` | One per `chunk` from the agent. `sequence` is 1, 2, 3... |
| `tool-call` | `{ messageId, toolCallId, name, argsJson, timestamp }` | One per `tool_call` from the agent. Zero or more, any point in the stream. `toolCallId` matches the corresponding `tool-result`. |
| `tool-result` | `{ messageId, toolCallId, status, ms, rowCount, timestamp }` | One per `tool_result` from the agent. `status` is `OK`/`FAILED`; `rowCount` is `null` unless `status` is `OK` and the agent reported one. |
| `payload` | `{ messageId, payload, timestamp }` | The structured analysis from §5, unwrapped and passed straight through. |
| `stream-complete` | `{ messageId, totalChunks, truncated, timestamp }` | `truncated` is `true` only if the agent cut generation short — the text already streamed is still coherent, just incomplete. |
| `error` | `{ messageId, errorCode, errorMessage, timestamp }` | Terminates the stream instead of `stream-complete`. |

Exactly one of `stream-complete` or `error` ends every stream — never both, never
neither. `tool-call`/`tool-result` are purely additive trace events; a stream with none
of them is still perfectly normal (the agent didn't need a tool for that answer).

## 7. Full example: a two-turn conversation

**Turn 1 — analyst opens a new chat:**

```bash
curl -N -X POST http://localhost:8080/api/v1/chat/messages \
  -H "Content-Type: application/json" -H "Accept: text/event-stream" \
  -H "X-Tenant-Id: tenant-42" -H "X-Org-Id: org-7" \
  -d '{"caseId":"case-1001","message":"Summarize this case for me"}'
```

```
event:stream-start
data:{"messageId":"m-1","timestamp":"..."}

event:message
data:{"messageId":"m-1","sequence":1,"content":"This case was...","timestamp":"..."}

... more "message" events ...

event:tool-call
data:{"messageId":"m-1","toolCallId":"t-1","name":"lookup_case_events","argsJson":"{\"caseId\":\"case-1001\"}","timestamp":"..."}

event:tool-result
data:{"messageId":"m-1","toolCallId":"t-1","status":"OK","ms":42,"rowCount":7,"timestamp":"..."}

event:payload
data:{"messageId":"m-1","payload":{...},"timestamp":"..."}

event:stream-complete
data:{"messageId":"m-1","totalChunks":5,"truncated":false,"timestamp":"..."}
```

The frontend appends both this turn's prompt and the assembled answer text (from the
`message` events) to its own local transcript — this backend never remembers any of it
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

Every failure — whether it happens before the ML Agent even responds, or partway
through streaming — surfaces as an `error` SSE event on the **same** stream. This
backend never returns an HTTP error status once streaming has started, so the
frontend should always watch for an `error` event rather than expecting an HTTP
status code mid-stream.

| `errorCode` | What happened | Should the frontend retry? |
|---|---|---|
| `VALIDATION_ERROR` | The request itself was invalid (missing `X-Tenant-Id`/`X-Org-Id` header, missing `caseId`, message too long, etc.) — this one *can* also arrive as an HTTP 400, before any SSE event, if the problem is caught immediately | No — fix the request |
| `NOT_FOUND` | The case wasn't found for that tenant | No |
| `ML_AGENT_REFUSED` | The ML Agent explicitly declined to process the request | No |
| `ML_AGENT_TIMEOUT` | The ML Agent took too long to respond | Maybe, after a pause |
| `ML_AGENT_UNAVAILABLE` | This backend couldn't reach the ML Agent at all | Maybe, after a pause |
| `ML_AGENT_ERROR` | The ML Agent reached an internal problem (e.g. its own data source was unavailable) | Maybe, after a pause |
| `CONCURRENCY_LIMIT_REACHED` | This backend is protecting itself/the ML Agent from overload right now | Yes, after a short pause |
| `INTERNAL_ERROR` | Something unexpected on this backend's side, or a credential/access problem between this backend and the ML Agent | No — this is not something the frontend can fix |

`errorMessage` is always a short, safe, human-readable sentence — never a raw
exception or stack trace.

## 9. Testing this without a real ML Agent

By default this backend runs against a **mock** ML Agent (no real agent needed —
great for frontend development). Trigger specific behaviors by including these
keywords anywhere in your `message` text:

| Keyword | Simulates |
|---|---|
| `trigger:slow` | A normal response, but much slower between chunks |
| `trigger:timeout` | The agent never responds — exercises your timeout/retry handling |
| `trigger:error` | A generic agent failure → `ML_AGENT_ERROR` |
| `trigger:empty` | A valid but empty response (no `message`/`payload` events, straight to `stream-complete`) |
| `trigger:rejected` | The agent rejects the request → `NOT_FOUND` |

Any other text gets a canned, realistic-looking fraud-case answer, including a valid
`payload` with citations — so the full contract (§5–§6) is exercisable end-to-end
without waiting on the real ML Agent integration.

## 10. Quick-reference field map

```text
FRONTEND SENDS               THIS BACKEND FORWARDS              ML AGENT RETURNS            THIS BACKEND RETURNS
─────────────────            ────────────────────────────       ─────────────────           ─────────────────────
X-Tenant-Id (header)  ───▶  requestContext.tenant
X-Org-Id (header) ▶ requestContext.organization
caseId            ───▶  caseContext.caseId
endUserId         ───▶  caseContext.endUserId
history           ───▶  history (role/content ──▶ user/agent oneof)
message           ───▶  prompt                     ◀───  chunk.delta         ───▶  message.content
X-User-Id         ───▶  operatorId                 ◀───  tool_call           ───▶  tool-call (toolCallId, name, argsJson)
requestId         ───▶  requestContext.agentSessionId  ◀── tool_result       ───▶  tool-result (toolCallId, status, ms, rowCount)
X-Correlation-Id  ───▶  requestContext.requestId    ◀───  ping               (never forwarded — logged only)
                                                          ◀───  payload             ───▶  payload.payload
                                                          ◀───  done               ───▶  stream-complete (totalChunks, truncated)
                                                          ◀───  error               ───▶  error.errorCode / errorMessage
```
