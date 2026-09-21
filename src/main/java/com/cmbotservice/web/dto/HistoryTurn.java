package com.cmbotservice.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * One turn of an explicit conversation transcript, as understood by {@link ChatRequest#history()}.
 * Forwarded to the ML Agent untouched — this backend never interprets {@code role}/{@code content},
 * just translates it into the wire contract's {@code user}/{@code agent} oneof shape (see {@code
 * GrpcMlAgentClient#toConversationTurn}).
 *
 * <p>{@code role}/{@code content} is this backend's own stable shape for a chat turn, not the wire
 * format — see README.md "Known limitations" for background.
 */
public record HistoryTurn(
    @NotBlank(message = "history[].role must not be blank")
        @Size(max = 20, message = "history[].role must be at most 20 characters")
        @Schema(
            description =
                "Who sent this turn — assumed \"user\" or \"assistant\" (unconfirmed, see README.md).",
            example = "user")
        String role,
    @NotBlank(message = "history[].content must not be blank")
        @Size(max = 4000, message = "history[].content must be at most 4000 characters")
        @Schema(
            description = "That turn's message text, forwarded as-is.",
            example = "Summarize this case for me")
        String content) {}
