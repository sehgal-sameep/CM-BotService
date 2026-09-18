package com.cmbotservice.sse;

/**
 * SSE {@code event:} names understood by chatbot clients. Part of the documented SSE
 * contract — see the OpenAPI description on {@code ChatController}.
 */
public final class SseEventType {

    public static final String STREAM_START = "stream-start";
    public static final String MESSAGE = "message";
    public static final String TOOL_CALL = "tool-call";
    public static final String TOOL_RESULT = "tool-result";
    public static final String PAYLOAD = "payload";
    public static final String STREAM_COMPLETE = "stream-complete";
    public static final String ERROR = "error";

    private SseEventType() {
    }
}
