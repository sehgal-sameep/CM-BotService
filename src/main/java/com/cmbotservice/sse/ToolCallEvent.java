package com.cmbotservice.sse;

import java.time.Instant;

/**
 * Payload for the {@link SseEventType#TOOL_CALL} event: the ML Agent invoked a tool while producing
 * its answer (the ML Agent's own {@code tool_call} event, translated to our field names). May be
 * sent zero or more times, at any point in the stream; {@code toolCallId} matches the corresponding
 * {@link ToolResultEvent}.
 */
public record ToolCallEvent(
    String messageId, String toolCallId, String name, String argsJson, Instant timestamp)
    implements ChatSseEvent {}
