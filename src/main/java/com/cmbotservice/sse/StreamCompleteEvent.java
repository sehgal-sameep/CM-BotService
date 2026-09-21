package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#STREAM_COMPLETE} event: the assistant's response finished.
 * {@code totalChunks} may be 0 for an empty response. {@code truncated} is true when the ML Agent
 * cut generation short (its {@code stop_reason}) — the text already streamed is still coherent,
 * just incomplete.
 *
 * <p>There is no conversation/continuation identifier to hand back here — the real ML Agent's
 * contract has none; the caller resumes a conversation by resending the full {@code history} (see
 * {@code ChatRequest}), which this backend never assembles, stores, or replays itself.
 */
public record StreamCompleteEvent(
    String messageId, int totalChunks, boolean truncated, Instant timestamp)
    implements ChatSseEvent {}
