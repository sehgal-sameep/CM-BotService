# Session Debug API — How to Call It

`GET /api/v1/debug/session-lookup` is a **temporary** verification endpoint
(`SessionDebugController`). It runs the exact Redis lookup that real chat
authentication uses (`SessionAuthenticationWebFilter` → `SessionStore.findSession`) and
returns the stored session record, so you can check that a session cookie and tenant
resolve correctly without sending a chat message.

It is **not** part of the stable API and will be removed once the auth flow no longer
needs manual checks.

## Prerequisites

- The app must run with `chatbot.security.mode: BFF_SESSION` (env var
  `CHATBOT_SECURITY_MODE=BFF_SESSION`). In the default `NONE` mode the endpoint doesn't
  exist and returns `404`.
- Redis must be reachable (`REDIS_HOST`, `REDIS_PORT`, … — see the README's
  Authentication section).
- You need two values:
  - **Session value**: the raw value of the BFF's `SESSION` cookie. In the FMC UI,
    open DevTools → Application → Cookies and copy the `SESSION` cookie's **Value**
    only, not `SESSION=` and not the whole `Cookie:` header.
  - **Tenant**: the caller's tenant id, sent as `X-Tenant-Id`.

The Redis key looked up is `session:<session value>:<tenant>`.

## Why Swagger UI's "Try it out" fails

Swagger UI shows a `SESSION` cookie field for this endpoint, but filling it in has no
effect. Browsers don't let page JavaScript set the `Cookie` request header (it's a
[forbidden request header](https://developer.mozilla.org/en-US/docs/Glossary/Forbidden_request_header)),
so the request goes out without the cookie. The server then correctly responds:

```json
{
  "errorCode": "VALIDATION_ERROR",
  "message": "Required cookie 'SESSION' is not present."
}
```

Swagger UI even shows a curl command with `-H 'Cookie: SESSION=…'` under the response.
That command is what Swagger *meant* to send, not what the browser actually sent.
Copying it into a terminal works.

The same limitation applies to `POST /api/v1/chat/messages` in `BFF_SESSION` mode.

## Option 1 — curl (recommended)

**Git Bash / macOS / Linux:**

```bash
curl -i "http://localhost:8080/back-office-ai/api/v1/debug/session-lookup" \
  -H "Cookie: SESSION=<session value>" \
  -H "X-Tenant-Id: <tenant>"
```

`--cookie` is equivalent:

```bash
curl -i "http://localhost:8080/back-office-ai/api/v1/debug/session-lookup" \
  --cookie "SESSION=<session value>" \
  -H "X-Tenant-Id: <tenant>"
```

**Windows PowerShell:** use `curl.exe`, not `curl`. In Windows PowerShell 5.1, `curl` is
an alias for `Invoke-WebRequest`, which takes different arguments. Put each argument on
one line, or use PowerShell's backtick for line continuation instead of `\`:

```powershell
curl.exe -i "http://localhost:8080/back-office-ai/api/v1/debug/session-lookup" `
  -H "Cookie: SESSION=<session value>" `
  -H "X-Tenant-Id: <tenant>"
```

**Windows cmd.exe:** one line, or `^` for continuation:

```bat
curl -i "http://localhost:8080/back-office-ai/api/v1/debug/session-lookup" -H "Cookie: SESSION=<session value>" -H "X-Tenant-Id: <tenant>"
```

Add `-H "X-Correlation-Id: <any id>"` to find this request in the logs easily. The
server echoes it back and tags every log line of the request with `corrId=<id>`. A
lookup logs `DEBUG_SESSION_LOOKUP_STARTED` → `REDIS_SESSION_LOOKUP_STARTED` (masked key
and the Redis endpoint in use) → `REDIS_SESSION_RECORD_FOUND`/`_PARSED` →
`DEBUG_SESSION_LOOKUP_FOUND`, or, when something's wrong, `REDIS_SESSION_RECORD_NOT_FOUND`,
`REDIS_SESSION_RECORD_INCOMPLETE` (names the missing field), or
`REDIS_SESSION_LOOKUP_FAILED` (endpoint and full cause chain, e.g. `Connection refused`).
Every endpoint's curl is collected in [`API_CURL_REFERENCE.md`](API_CURL_REFERENCE.md).

## Option 2 — keep using Swagger UI

Browsers *do* attach cookies automatically on same-origin requests, so set the cookie
on Swagger's own origin first:

1. Open `http://localhost:8080/swagger-ui.html`.
2. Open DevTools → Console and run:
   ```js
   document.cookie = "SESSION=<session value>; path=/";
   ```
3. In "Try it out", leave the `SESSION` cookie field empty (it's ignored anyway), fill
   in `X-Tenant-Id`, and click Execute.

This only works when Swagger UI is served from the same host and port as the API (as it
is locally). The cookie stays until you close the browser. Remove it afterwards with
`document.cookie = "SESSION=; path=/; max-age=0";`.

## Option 3 — Postman / Insomnia

Add a header `Cookie: SESSION=<session value>` (or use the tool's cookie manager for
`localhost`) plus `X-Tenant-Id: <tenant>`. Desktop API clients aren't subject to the
browser's forbidden-header rule.

## Responses

| Status | Meaning |
|---|---|
| `200` | A record exists at `session:<session value>:<tenant>`. Body: `{ username, tenantId, accessToken, refreshToken, contextJson, fingerprint }`, exactly as stored and not validated. |
| `400 VALIDATION_ERROR` | The `SESSION` cookie or `X-Tenant-Id` header didn't reach the server (see "Why Swagger UI fails" above), or was blank. |
| `401 UNAUTHENTICATED` | Both values arrived, but no Redis record exists for that pair. Check that the tenant matches the session's tenant, that the session hasn't expired in the BFF, and that this service points at the same Redis as the BFF. |
| `404` | The app isn't running in `chatbot.security.mode: BFF_SESSION`. |
| `503 SESSION_STORE_UNAVAILABLE` | Redis is unreachable. First check the startup line `REDIS_SESSION_STORE_REACHABLE`/`_UNREACHABLE`, which PINGs Redis (including the Entra ID token fetch) as soon as the app is up. Check the `REDIS_SESSION_LOOKUP_FAILED` log line: its `endpoint=` shows the host/port/TLS/auth mode actually in use, and the last entry of `causeChain` is the real reason (connection refused, timeout, TLS handshake, auth). |

## Handle the output carefully

A `200` response contains the session's live **access token and refresh token**. Don't
paste it into chat, tickets, screenshots, or shared docs, and don't commit the curl
command with a real session value filled in. If you need to share a result, share only
the status code and the `username`/`tenantId` fields.
