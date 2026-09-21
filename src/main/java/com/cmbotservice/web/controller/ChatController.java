package com.cmbotservice.web.controller;

import com.cmbotservice.context.RequestContext;
import com.cmbotservice.context.RequestContextResolver;
import com.cmbotservice.service.ChatOrchestrationService;
import com.cmbotservice.web.ApiPaths;
import com.cmbotservice.web.dto.ChatRequest;
import com.cmbotservice.web.dto.ErrorResponse;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Flux;

/**
 * The one endpoint this stateless orchestrator exposes: forward a chat message to the ML Agent and
 * stream its response back over SSE. There is no conversation resource here to create, fetch, or
 * close — see {@link ChatRequest} and the SSE contract documented on {@link #sendMessage}. Fully
 * non-blocking: the returned {@code Flux} is subscribed and written by Reactor Netty as elements
 * arrive, never buffered.
 */
@RestController
@Tag(
    name = "Chat",
    description =
        "Stateless chat message orchestration — forwards each message to the "
            + "ML Agent (through a circuit breaker, bulkhead, timeout, and limited retry) and streams its "
            + "response over SSE. This backend holds no conversation state between requests; conversation "
            + "memory (if any) is owned by the ML Agent.")
public class ChatController {

  private final ChatOrchestrationService chatOrchestrationService;
  private final RequestContextResolver requestContextResolver;

  public ChatController(
      ChatOrchestrationService chatOrchestrationService,
      RequestContextResolver requestContextResolver) {
    this.chatOrchestrationService = chatOrchestrationService;
    this.requestContextResolver = requestContextResolver;
  }

  @PostMapping(value = ApiPaths.CHAT_MESSAGES, produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  @Operation(
      summary = "Send a chat message and stream the ML Agent's response over SSE",
      description =
          """
                    Stateless pass-through: this endpoint validates the request, forwards it to \
                    the ML Agent (through a circuit breaker, bulkhead, timeout, and limited retry — \
                    see the architecture doc), and streams the response back. It does not store the \
                    message, the response, or any conversation history — nothing here persists \
                    between requests.

                    `X-Tenant-Id` and `X-Org-Id` request headers, and the body's `caseId`/ \
                    `message`, are required. The ML Agent's contract has no conversation/continuation \
                    identifier at all — `history` is the sole resumption mechanism: omit it to start \
                    a new conversation, or resend the full transcript (oldest turn first) to continue \
                    one. `requestId` is an optional caller-generated identifier forwarded for \
                    tracing/correlation only (idempotency-friendly, not deduplicated anywhere). The \
                    caller — not this backend — is responsible for remembering and resending `history`.

                    SSE event contract (in order):
                    - `stream-start` — { messageId, timestamp }
                    - any mix of, zero or more times, in the order the ML Agent produced them:
                      - `message` — { messageId, sequence, content, timestamp }
                      - `tool-call` — { messageId, toolCallId, name, argsJson, timestamp } — the ML \
                    Agent invoked a tool while producing its answer; `argsJson` is the tool's \
                    arguments serialized as a JSON string. `toolCallId` matches the `tool-result` \
                    for the same invocation.
                      - `tool-result` — { messageId, toolCallId, status, ms, rowCount, timestamp } — \
                    the outcome of a tool invocation. `status` is `OK` or `FAILED` (any value the ML \
                    Agent doesn't recognize is also reported as `FAILED`). `ms` is the tool's \
                    execution duration. `rowCount` is the number of rows/items the tool returned, \
                    present only when `status` is `OK` and the ML Agent reported one, `null` otherwise.
                    - `payload` (at most one) — { messageId, payload: { keySignals, citations }, timestamp }
                    - exactly one of:
                      - `stream-complete` — { messageId, totalChunks, truncated, timestamp }
                      - `error` — { messageId, errorCode, errorMessage, timestamp }

                    `errorCode` is one of ML_AGENT_TIMEOUT, ML_AGENT_UNAVAILABLE, ML_AGENT_ERROR, \
                    ML_AGENT_REFUSED, CONCURRENCY_LIMIT_REACHED (bulkhead full or circuit breaker \
                    open), INTERNAL_ERROR. Because the HTTP status is already committed to 200 by \
                    the time any of these can occur, every post-acceptance failure — including a \
                    rejected/timed-out ML Agent call — surfaces as an `error` event on this same \
                    stream, never a different HTTP status.

                    For local testing, the mock ML Agent recognizes these keywords anywhere in \
                    `message` to simulate each failure mode: `trigger:slow`, `trigger:timeout`, \
                    `trigger:error`, `trigger:empty`, `trigger:rejected`.

                    Swagger UI's "Try it out" does not render a live SSE stream well (it waits \
                    for the connection to end, then shows the buffered body). To watch events \
                    arrive in real time, use curl instead:

                    ```
                    curl -N -X POST "http://localhost:8080/api/v1/chat/messages" \\
                      -H "Content-Type: application/json" \\
                      -H "Accept: text/event-stream" \\
                      -H "X-User-Id: analyst-1" \\
                      -H "X-Tenant-Id: tenant-123" \\
                      -H "X-Org-Id: org-123" \\
                      -d '{"caseId":"case-456","message":"Summarize this case for me"}'
                    ```
                    """)
  @ApiResponse(
      responseCode = "200",
      description = "SSE stream of chatbot events",
      content = @Content(mediaType = MediaType.TEXT_EVENT_STREAM_VALUE))
  @ApiResponse(
      responseCode = "400",
      description =
          "Validation error (e.g. blank/missing X-Tenant-Id, " + "X-Org-Id, caseId, or message)",
      content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
  public Flux<ServerSentEvent<Object>> sendMessage(
      @RequestBody @Valid ChatRequest request, ServerWebExchange exchange) {
    RequestContext context = requestContextResolver.resolve(exchange, request.caseId());
    return chatOrchestrationService.streamMessage(context, request);
  }
}
