package com.cmbotservice.common;

/**
 * The ML Agent explicitly rejected the request for a non-transient reason — bad/expired
 * service credential (401), tenant not permitted (403), case not found in that tenant
 * (404), or a malformed request/missing required context (422). These arrive as the
 * HTTP status of the initial {@code POST /v1/chat} call, before any SSE byte is
 * written. Never retryable: the agent understood the request and said no, so retrying
 * unchanged would just get the same answer again.
 * <p>
 * Carries its own {@link ErrorCode} rather than always mapping to one fixed code,
 * since the right code genuinely differs by which status this wraps (see
 * {@code HttpMlAgentClient} for the mapping).
 */
public class MlAgentRejectedException extends MlAgentException {

    private final ErrorCode errorCode;

    public MlAgentRejectedException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ErrorCode errorCode() {
        return errorCode;
    }
}
