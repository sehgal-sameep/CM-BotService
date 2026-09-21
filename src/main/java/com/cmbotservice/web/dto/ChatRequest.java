package com.cmbotservice.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;

/**
 * The single request shape for this stateless orchestrator: everything the ML Agent needs for one
 * message, self-contained. There is nothing to look up server-side — {@code caseId}/{@code history}
 * are just forwarded, not validated against any stored record. {@code tenantId}/{@code
 * organization} are <b>not</b> here — the frontend sends those as the {@code X-Tenant-Id}/{@code
 * X-Org-Id} request headers instead (see {@code RequestContextResolver}), not the body.
 */
public record ChatRequest(
    @NotBlank(message = "caseId must not be blank")
        @Size(max = 100, message = "caseId must be at most 100 characters")
        @Pattern(
            regexp = "^[A-Za-z0-9_-]+$",
            message = "caseId may only contain letters, digits, '_' and '-'")
        @Schema(
            description = "Case identifier, forwarded as-is to the ML Agent.",
            example = "case-456")
        String caseId,
    @Valid
        @Size(max = 50, message = "history must contain at most 50 turns")
        @Schema(
            description =
                "Explicit conversation transcript, oldest turn first. The ML Agent's contract has "
                    + "no continuation token or conversation id — resending the full transcript on every "
                    + "message is the only resumption mechanism. Omit (or send empty) to start a new "
                    + "conversation. This backend does not assemble, store, or interpret this transcript — "
                    + "the caller owns remembering and resending it.",
            requiredMode = Schema.RequiredMode.NOT_REQUIRED)
        List<HistoryTurn> history,
    @Size(max = 100, message = "requestId must be at most 100 characters")
        @Schema(
            description =
                "Optional caller-generated request identifier, forwarded to the ML Agent "
                    + "and included in logs for tracing/correlation only (idempotency-friendly by "
                    + "design, but not enforced or deduplicated anywhere in this stateless service).",
            example = "req-a1b2c3",
            requiredMode = Schema.RequiredMode.NOT_REQUIRED)
        String requestId,
    @Size(max = 200, message = "endUserId must be at most 200 characters")
        @Schema(
            description =
                "Optional hint forwarded as-is to the ML Agent's case context. Exact "
                    + "semantics are defined by the ML Agent's contract, not this backend — forwarded "
                    + "untouched, never interpreted or defaulted here.",
            example = "gadi5",
            requiredMode = Schema.RequiredMode.NOT_REQUIRED)
        String endUserId,
    @NotBlank(message = "message must not be blank")
        @Size(max = 4000, message = "message must be at most 4000 characters")
        @Schema(
            description =
                "The analyst's message — either a predefined prompt or free text. "
                    + "For local testing, the mock ML Agent recognizes the keywords "
                    + "'trigger:slow', 'trigger:timeout', 'trigger:error', 'trigger:empty', and "
                    + "'trigger:rejected' anywhere in this text to simulate that scenario.",
            example = "Summarize this case for me")
        String message) {}
