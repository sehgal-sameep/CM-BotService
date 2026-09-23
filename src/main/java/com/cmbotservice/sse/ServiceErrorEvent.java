package com.cmbotservice.sse;

import com.cmbotservice.common.ErrorCode;
import java.time.Instant;

/**
 * Payload for the {@link SseEventType#SERVICE_ERROR} event — the one event on the stream that does
 * <b>not</b> come from the ML Agent. It exists only because some failures never produce an ML Agent
 * event at all (the agent could not be reached, timed out, rejected the call at the gRPC level, or
 * this backend's own circuit breaker/bulkhead/validation stopped the request), yet the HTTP status
 * is already committed to 200 by then. The ML Agent's own model-level {@code error} event is never
 * converted into this; it is forwarded untouched like every other agent event.
 *
 * <p>{@code errorMessage} is always a generic, client-safe description — internal exception
 * details/stack traces are logged server-side only, never sent to the client. {@code messageId} is
 * this backend's per-request log correlation id.
 */
public record ServiceErrorEvent(
    String messageId, ErrorCode errorCode, String errorMessage, Instant timestamp) {}
