package com.cmbotservice.sse;

import org.springframework.http.codec.ServerSentEvent;

/**
 * Converts a {@link ChatSseEvent} into the {@code ServerSentEvent} Spring actually
 * writes to the response. A pure mapping function — no state, no I/O — unlike the
 * imperative {@code SseStreamWriter} this replaces; sequencing across event types is
 * done once, upstream, via {@code Flux#index()} in {@code ChatOrchestrationService},
 * and handed in here as {@code frameId} rather than tracked with a mutable counter.
 */
public final class SseEvents {

    private SseEvents() {
    }

    public static ServerSentEvent<Object> toServerSentEvent(long frameId, ChatSseEvent event) {
        String eventName = switch (event) {
            case StreamStartEvent ignored -> SseEventType.STREAM_START;
            case MessageChunkEvent ignored -> SseEventType.MESSAGE;
            case ToolCallEvent ignored -> SseEventType.TOOL_CALL;
            case ToolResultEvent ignored -> SseEventType.TOOL_RESULT;
            case CaseSummaryEvent ignored -> SseEventType.PAYLOAD;
            case StreamCompleteEvent ignored -> SseEventType.STREAM_COMPLETE;
            case StreamErrorEvent ignored -> SseEventType.ERROR;
        };
        return ServerSentEvent.<Object>builder()
                .id(String.valueOf(frameId))
                .event(eventName)
                .data(event)
                .build();
    }
}
