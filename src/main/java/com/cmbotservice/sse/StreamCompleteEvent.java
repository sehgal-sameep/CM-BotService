package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#STREAM_COMPLETE} event: the assistant's response
 * finished successfully. {@code totalChunks} may be 0 for an empty response.
 * <p>
 * {@code conversationId} and {@code continuation} here <b>are</b> authoritative — the
 * real ML Agent only reveals them on its final {@code done} event, which this maps
 * directly from. The caller must send whichever of these it received back on its next
 * message (see {@code ChatRequest}) — this backend does not remember either one.
 */
public record StreamCompleteEvent(
        String conversationId,
        String continuation,
        String messageId,
        int totalChunks,
        Instant timestamp
) implements ChatSseEvent {
}
