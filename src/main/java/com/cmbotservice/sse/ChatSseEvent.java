package com.cmbotservice.sse;

/**
 * Marker for the five SSE payload records that make up this service's outbound event
 * contract. Sealing them enables one exhaustive, compiler-checked {@code switch} at
 * the single place they're converted to a {@code ServerSentEvent}
 * ({@code ChatOrchestrationService}), and lets that conversion number every event
 * uniformly via {@code Flux#index()} instead of a hand-maintained counter.
 */
public sealed interface ChatSseEvent
        permits StreamStartEvent, MessageChunkEvent, CaseSummaryEvent, StreamCompleteEvent, StreamErrorEvent {
}
