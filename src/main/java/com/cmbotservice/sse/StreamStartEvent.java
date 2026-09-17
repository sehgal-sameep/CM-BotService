package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#STREAM_START} event: the ML Agent has accepted
 * the request and streaming is about to begin. Carries nothing but identifiers —
 * the real contract has no conversation/continuation identifier to echo; {@code history}
 * on the next request is the caller's sole resumption mechanism.
 */
public record StreamStartEvent(String messageId, Instant timestamp) implements ChatSseEvent {
}
