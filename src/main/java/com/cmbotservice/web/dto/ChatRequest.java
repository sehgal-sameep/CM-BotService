package com.cmbotservice.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

import java.util.List;

/**
 * The single request shape for this stateless orchestrator: everything the ML Agent
 * needs for one message, self-contained. There is nothing to look up server-side —
 * {@code tenantId}/{@code caseId}/{@code conversationId}/{@code continuation}/
 * {@code history} are just forwarded, not validated against any stored record.
 */
public record ChatRequest(

        @NotBlank(message = "tenantId must not be blank")
        @Size(max = 100, message = "tenantId must be at most 100 characters")
        @Pattern(regexp = "^[A-Za-z0-9_-]+$", message = "tenantId may only contain letters, digits, '_' and '-'")
        @Schema(description = "Tenant identifier, forwarded as-is to the ML Agent.", example = "tenant-123")
        String tenantId,

        @NotBlank(message = "caseId must not be blank")
        @Size(max = 100, message = "caseId must be at most 100 characters")
        @Pattern(regexp = "^[A-Za-z0-9_-]+$", message = "caseId may only contain letters, digits, '_' and '-'")
        @Schema(description = "Case identifier, forwarded as-is to the ML Agent.", example = "case-456")
        String caseId,

        @Size(max = 200, message = "conversationId must be at most 200 characters")
        @Schema(
                description = "Conversation identifier from a prior response's `stream-complete` event. "
                        + "Omit (or send blank) to start a new conversation. Sent alongside `continuation` "
                        + "if both are known — the ML Agent's own contract says the caller never needs to "
                        + "choose between them, just echo back whatever the last response returned. This "
                        + "backend does not store or interpret this value.",
                example = "conversation-789",
                requiredMode = Schema.RequiredMode.NOT_REQUIRED
        )
        String conversationId,

        @Size(max = 4096, message = "continuation must be at most 4096 characters")
        @Schema(
                description = "Opaque continuation token from a prior response's `stream-complete` event "
                        + "(the ML Agent's preferred mechanism for resuming a conversation, taking "
                        + "precedence over `conversationId` if both are sent). Not human-readable, not "
                        + "interpreted by this backend — just round-tripped.",
                example = "v1.k3.eyJlbmMiOi...",
                requiredMode = Schema.RequiredMode.NOT_REQUIRED
        )
        String continuation,

        @Valid
        @Size(max = 50, message = "history must contain at most 50 turns")
        @Schema(
                description = "Explicit conversation transcript from a prior response's `stream-complete` event "
                        + "onward — the ML Agent's own contract lists `history` as its highest-precedence "
                        + "resumption mechanism (history > continuation > conversationId). Omit for a new "
                        + "conversation. This backend does not assemble, store, or interpret this transcript — "
                        + "the caller owns remembering and resending it, exactly like `continuation`/"
                        + "`conversationId`, so forwarding it does not compromise this service's stateless design.",
                requiredMode = Schema.RequiredMode.NOT_REQUIRED
        )
        List<HistoryTurn> history,

        @Size(max = 100, message = "requestId must be at most 100 characters")
        @Schema(
                description = "Optional caller-generated request identifier, forwarded to the ML Agent "
                        + "and included in logs for tracing/correlation. Idempotency-friendly by "
                        + "design, but not enforced or deduplicated anywhere in this stateless service.",
                example = "req-a1b2c3",
                requiredMode = Schema.RequiredMode.NOT_REQUIRED
        )
        String requestId,

        @Size(max = 200, message = "endUserId must be at most 200 characters")
        @Schema(
                description = "Optional hint forwarded as-is to the ML Agent's `context.endUserId`. Exact "
                        + "semantics are defined by the ML Agent's contract, not this backend — forwarded "
                        + "untouched, never interpreted or defaulted here.",
                example = "gadi5",
                requiredMode = Schema.RequiredMode.NOT_REQUIRED
        )
        String endUserId,

        @NotBlank(message = "message must not be blank")
        @Size(max = 4000, message = "message must be at most 4000 characters")
        @Schema(
                description = "The analyst's message — either a predefined prompt or free text. "
                        + "For local testing, the mock ML Agent recognizes the keywords "
                        + "'trigger:slow', 'trigger:timeout', 'trigger:error', 'trigger:empty', "
                        + "'trigger:rejected', and 'trigger:continuation-expired' anywhere in this "
                        + "text to simulate that scenario.",
                example = "Summarize this case for me"
        )
        String message
) {
}
