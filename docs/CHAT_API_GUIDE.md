# Chat API Guide — How a Message Flows Through This Backend

A plain-English walkthrough of the one API this service exposes: what the frontend
sends us, what we forward to the ML Agent, what the ML Agent streams back, and what we
stream back to the frontend. No code required to read this — just JSON examples.

For the "why" behind design decisions, see [`ARCHITECTURE.md`](ARCHITECTURE.md). This
doc is the "what" — a reference you'd hand to a frontend developer or a new teammate.

## 1. The big picture

```
┌──────────┐   1. one HTTP POST    ┌─────────────┐   2. one HTTP POST   ┌───────────┐
│          │  ───────────────────▶ │             │ ───────────────────▶│           │
│ Frontend │                        │ This Backend│                      │ ML Agent  │
│ (chat UI)│  ◀─────────────────── │ (this repo) │ ◀───────────────────│ (Thoughtful│
│          │   4. SSE stream back   │             │   3. SSE stream back │  Labs)    │
└──────────┘                        └─────────────┘                      └───────────┘
```

This backend never stores anything. It takes one message in, forwards it, and streams
the answer straight back out — a translator sitting between two different contracts,
not a database or a session manager. Every request is independent; nothing is
remembered between them except what the frontend chooses to send back on the next
message (see §5, "continuing a conversation").

## 2. Key terms (read this before the rest)

| Term | What it means |
|---|---|
| `tenantId` | Which customer/organization this request belongs to. |
| `caseId` | Which fraud case the analyst is chatting about. |
| `conversationId` / `continuation` | Two different "remember our last chat" tokens the ML Agent hands back. Treat both as opaque strings — **never inspect or generate them yourself**, just echo back whatever the last response gave you. |
| `messageId` | A unique ID this backend generates per message, for tracing in logs. Not something the frontend sends. |
| `requestId` | Optional, frontend-generated. Purely for your own tracing/support tickets — this backend logs it but doesn't use it for anything else. |
| `endUserId` | Optional hint about who the message is really about/for (exact meaning is the ML Agent's call, not this backend's). |
| `surface` | Which product is calling the ML Agent. This backend always sends `"case_manager"` — you never set this. |

## 3. Step 1 — What the frontend sends to this backend

**One endpoint, one method:**

```
POST /api/v1/chat/messages
Content-Type: application/json
Accept: text/event-stream
```

**Optional headers:**

| Header | Purpose |
|---|---|
| `X-User-Id` | The analyst's identity (stand-in until real login/auth exists). |
| `X-Correlation-Id` | Your own tracing ID — if you don't send one, this backend generates one and echoes it back on the response header. |

**Request body:**

```json
{
  "tenantId": "tenant-42",
  "caseId": "case-1001",
  "conversationId": null,
  "continuation": null,
  "requestId": "req-a1b2c3",
  "endUserId": null,
  "message": "Summarize this case for me"
}
```

| Field | Required? | Notes |
|---|---|---|
| `tenantId` | **Yes** | Letters, digits, `_`, `-` only. Max 100 chars. |
| `caseId` | **Yes** | Same charset rule. Max 100 chars. |
| `conversationId` | No | Omit (or `null`) to start a brand-new conversation. Otherwise, send back the exact value from the previous response's `stream-complete` event. |
| `continuation` | No | Same idea as `conversationId` — a second "remember our chat" token. Send back both if you have both; the ML Agent doesn't need you to choose between them. |
| `requestId` | No | Your own tracking ID, for your logs only. |
| `endUserId` | No | Forwarded to the ML Agent as-is; not interpreted by this backend. |
| `message` | **Yes** | The analyst's question/prompt. Max 4000 characters. |

**That's it — this is the entire contract the frontend needs to know.** Everything
past this point (§4–§6) happens inside this backend and is invisible to the frontend;
skip to §7 if you only care about what you send/receive.

## 4. Step 2 — What this backend forwards to the ML Agent

This backend re-shapes your request into the ML Agent's own contract
(`POST /v1/chat`) before forwarding it. You never see this directly, but it's useful
to know the mapping if something looks wrong end-to-end:

```json
{
  "continuation": null,
  "conversationId": null,
  "surface": "case_manager",
  "message": "Summarize this case for me",
  "context": {
    "caseId": "case-1001",
    "endUserId": null
  },
  "tenantId": "tenant-42",
  "options": {
    "includeResolutions": true
  }
}
```

| Your field | Becomes | Notes |
|---|---|---|
| `tenantId` | `tenantId` | Passed straight through. |
| `caseId` | `context.caseId` | Nested under `context`. |
| `endUserId` | `context.endUserId` | Nested under `context`. |
| `conversationId` / `continuation` | same names, top-level | Both echoed through untouched. |
| `message` | `message` | Untouched. |
| — | `surface: "case_manager"` | Always this fixed value — this backend hardcodes it. |
| — | `options.includeResolutions` | A server-side setting (`ml-agent.include-resolutions` in config), not something the frontend controls. |

Fields that **never** leave this backend: `X-User-Id`, `X-Correlation-Id`,
`requestId`, `messageId`. Those exist purely for this backend's own logging/tracing —
`correlationId`/`tenantId`/`caseId`/`conversationId` are already enough to trace one
chatbot interaction end to end, so no separate tracing token is needed.

## 5. Step 3 — What the ML Agent streams back

The ML Agent responds with a live stream of Server-Sent Events. Six possible event
types, always in roughly this order:

| Event | Meaning | How many? |
|---|---|---|
| `token` | One small chunk of the answer text, arriving live as it's generated | Many |
| `tool_call` | The agent looked something up internally (a DB query, a rules check, etc.) | 0 or more — **this backend swallows these**, they never reach the frontend |
| `tool_result` | The result of that lookup | One per `tool_call` — also swallowed |
| `payload` | The final, structured, cited analysis (see below) | Exactly one, right before the stream ends |
| `done` | "I'm finished" — carries the conversation tokens to remember for next time | One, always last on success |
| `error` | "Something went wrong" | Only appears instead of `done`, never alongside it |

**The `payload` event** is the important one — it's the structured answer for the
case manager UI to render:

```json
{
  "answer": "This case was created because...",
  "summary": {
    "narrative": "This case was created because...",
    "keySignals": [
      { "signal": "Unusual device/IP", "severity": "high", "citations": ["evt-123"] }
    ],
    "entities": [
      { "type": "ip", "value": "203.0.113.42", "events": ["evt-123"] }
    ],
    "timeline": [
      { "at": "2026-09-01T10:00:00Z", "eventId": "evt-123", "what": "Transaction flagged" }
    ]
  },
  "suggestedResolution": {
    "mark": "S",
    "label": "Suspected Fraud",
    "confidence": "medium",
    "rationale": "Multiple fraud indicators with no legitimate explanation."
  },
  "citations": [
    { "id": "evt-123", "source": "APP_EVENT_LOG", "fields": ["risk_score"] }
  ]
}
```

Two rules this backend enforces on every `payload` it receives (and rejects the
response if either is broken): every `keySignal` must point to at least one
`citations` entry, and `suggestedResolution.mark` must be one of
`F S G A U Y B T X C`.

**The `done` event** — the only place the ML Agent reveals the tokens to remember:

```json
{ "conversationId": "conv-abc123", "continuation": "v1.k3.eyJlbmMi...", "latencyMs": 1800, "tokensIn": 12, "tokensOut": 140 }
```

## 6. Step 4 — What this backend streams back to the frontend

This backend re-shapes the ML Agent's events into its own, simpler frontend-facing
contract — same events, cleaner field names, and the internal `tool_call`/
`tool_result` chatter removed entirely:

| Event | Payload | Notes |
|---|---|---|
| `stream-start` | `{ conversationId, messageId, timestamp }` | Sent immediately. `conversationId` here is just an echo of what you sent — **not confirmed yet**. |
| `message` | `{ conversationId, messageId, sequence, content, timestamp }` | One per `token` from the agent. `sequence` is 1, 2, 3... |
| `payload` | `{ conversationId, messageId, payload, timestamp }` | The structured analysis from §5, unwrapped and passed straight through. |
| `stream-complete` | `{ conversationId, continuation, messageId, totalChunks, timestamp }` | **The only place `conversationId`/`continuation` are confirmed/authoritative.** Save both — you'll send them back on the next message. |
| `error` | `{ conversationId, messageId, errorCode, errorMessage, timestamp }` | Terminates the stream instead of `stream-complete`. |

Exactly one of `stream-complete` or `error` ends every stream — never both, never
neither.

## 7. Full example: a two-turn conversation

**Turn 1 — analyst opens a new chat:**

```bash
curl -N -X POST http://localhost:8080/api/v1/chat/messages \
  -H "Content-Type: application/json" -H "Accept: text/event-stream" \
  -d '{"tenantId":"tenant-42","caseId":"case-1001","message":"Summarize this case for me"}'
```

```
event:stream-start
data:{"conversationId":null,"messageId":"m-1","timestamp":"..."}

event:message
data:{"conversationId":null,"messageId":"m-1","sequence":1,"content":"This case was...","timestamp":"..."}

... more "message" events ...

event:payload
data:{"conversationId":null,"messageId":"m-1","payload":{...},"timestamp":"..."}

event:stream-complete
data:{"conversationId":"conv-abc123","continuation":"v1.k3.eyJ...","messageId":"m-1","totalChunks":5,"timestamp":"..."}
```

The frontend stores `conv-abc123` and `v1.k3.eyJ...` (e.g. in the chat window's local
state — this backend never remembers them for you).

**Turn 2 — follow-up question, same conversation:**

```bash
curl -N -X POST http://localhost:8080/api/v1/chat/messages \
  -H "Content-Type: application/json" -H "Accept: text/event-stream" \
  -d '{"tenantId":"tenant-42","caseId":"case-1001","conversationId":"conv-abc123","continuation":"v1.k3.eyJ...","message":"Which rules were triggered?"}'
```

Same event sequence as Turn 1, except `stream-start`'s `conversationId` now echoes
`conv-abc123` (still unconfirmed until `stream-complete` repeats it).

## 8. When things go wrong

Every failure — whether it happens before the ML Agent even responds, or partway
through streaming — surfaces as an `error` SSE event on the **same** stream. This
backend never returns an HTTP error status once streaming has started, so the
frontend should always watch for an `error` event rather than expecting an HTTP
status code mid-stream.

| `errorCode` | What happened | Should the frontend retry? |
|---|---|---|
| `VALIDATION_ERROR` | The request itself was invalid (missing `tenantId`, message too long, etc.) — this one *can* also arrive as an HTTP 400, before any SSE event, if the problem is caught immediately | No — fix the request |
| `NOT_FOUND` | The case wasn't found for that tenant | No |
| `CONTINUATION_EXPIRED` | The `conversationId`/`continuation` you sent is stale or invalid | No — **discard it and start a new conversation** |
| `ML_AGENT_TIMEOUT` | The ML Agent took too long to respond | Maybe, after a pause |
| `ML_AGENT_UNAVAILABLE` | This backend couldn't reach the ML Agent at all | Maybe, after a pause |
| `ML_AGENT_ERROR` | The ML Agent reached an internal problem (rate-limited, temporarily down) | Maybe, after a pause |
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
| `trigger:continuation-expired` | Your `conversationId`/`continuation` is treated as stale → `CONTINUATION_EXPIRED` |

Any other text gets a canned, realistic-looking fraud-case answer, including a valid
`payload` with citations — so the full contract (§5–§6) is exercisable end-to-end
without waiting on the real ML Agent integration.

## 10. Quick-reference field map

```
FRONTEND SENDS          THIS BACKEND FORWARDS         ML AGENT RETURNS            THIS BACKEND RETURNS
─────────────────       ───────────────────────       ─────────────────           ─────────────────────
tenantId          ───▶  tenantId                                                  
caseId            ───▶  context.caseId                                           
endUserId         ───▶  context.endUserId                                        
conversationId    ───▶  conversationId          ◀───  done.conversationId  ───▶  stream-start / stream-complete.conversationId
continuation      ───▶  continuation            ◀───  done.continuation   ───▶  stream-complete.continuation
message           ───▶  message                 ◀───  token.delta         ───▶  message.content
                        surface: "case_manager"  ◀───  payload             ───▶  payload.payload
                        options.includeResolutions ◀── tool_call/tool_result     (never forwarded — logged only)
                                                  ◀───  error               ───▶  error.errorCode / errorMessage
requestId         (logged only, never forwarded)
X-User-Id / X-Correlation-Id  (logged only, never forwarded)
```
