package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#STREAM_START} event: the ML Agent has accepted
 * the request and streaming is about to begin.
 * <p>
 * {@code conversationId} here is <b>not authoritative</b> — the real ML Agent never
 * reveals a conversation identifier until its final {@code done} event, so this is
 * simply an echo of whatever the request itself carried (or {@code null} for a new
 * conversation). The confirmed value only appears on {@link StreamCompleteEvent}.
 */
public record StreamStartEvent(String conversationId, String messageId, Instant timestamp) implements ChatSseEvent {
}
