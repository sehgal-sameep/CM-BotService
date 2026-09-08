package com.cmbotservice.sse;

import com.cmbotservice.mlagent.CaseSummaryPayload;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#PAYLOAD} event — the structured, cited case
 * analysis (the ML Agent's own {@code payload} event, translated directly since its
 * shape is already exactly what the case manager UI needs: narrative, key signals with
 * citations, entities, timeline, and a suggested resolution). Sent exactly once, always
 * before {@link StreamCompleteEvent}, mirroring the ML Agent's own ordering guarantee.
 */
public record CaseSummaryEvent(
        String conversationId,
        String messageId,
        CaseSummaryPayload payload,
        Instant timestamp
) implements ChatSseEvent {
}
