package com.cmbotservice.sse;

import com.cmbotservice.common.ErrorCode;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#ERROR} event. {@code errorMessage} is always a
 * generic, client-safe description — internal exception details/stack traces are
 * logged server-side only, never sent to the client.
 */
public record StreamErrorEvent(
        String messageId,
        ErrorCode errorCode,
        String errorMessage,
        Instant timestamp
) implements ChatSseEvent {
}
