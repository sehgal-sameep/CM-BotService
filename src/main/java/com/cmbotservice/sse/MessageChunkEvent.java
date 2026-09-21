package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#MESSAGE} event: one chunk of the assistant's streamed answer
 * text (the ML Agent's own {@code chunk}/{@code delta}, translated to our field names). {@code
 * sequence} is 1-based and strictly increasing per message.
 */
public record MessageChunkEvent(String messageId, int sequence, String content, Instant timestamp)
    implements ChatSseEvent {}
