package com.cmbotservice.sse;

import com.cmbotservice.mlagent.MlAgentStreamEvent;
import java.time.Instant;

/**
 * Payload for the {@link SseEventType#TOOL_RESULT} event: the outcome of a tool invocation (the ML
 * Agent's own {@code tool_result} event, translated to our field names). {@code toolCallId} matches
 * the {@link ToolCallEvent} that triggered it. {@code rowCount} is {@code null} when the ML Agent's
 * {@code result_summary} oneof was unset (e.g. on failure).
 */
public record ToolResultEvent(
    String messageId,
    String toolCallId,
    MlAgentStreamEvent.ToolResult.Status status,
    long ms,
    Long rowCount,
    Instant timestamp)
    implements ChatSseEvent {}
