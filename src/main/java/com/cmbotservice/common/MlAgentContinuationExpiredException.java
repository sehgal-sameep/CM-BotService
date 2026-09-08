package com.cmbotservice.common;

/**
 * The ML Agent's {@code continuation}/{@code conversationId} was expired (4221) or
 * undecryptable/tampered (4222) — can only be discovered once the agent is mid-stream
 * (continuation validity isn't known until the agent actually looks at it), so this
 * always arrives via the terminal {@code error} SSE event, never as a pre-stream HTTP
 * status. Kept distinct from the more generic {@link MlAgentRejectedException} because
 * the frontend remediation is different and specific: discard the stored
 * continuation/conversationId and start a brand new conversation. Never retryable —
 * retrying with the same (bad) continuation would just fail again.
 */
public class MlAgentContinuationExpiredException extends MlAgentException {

    public MlAgentContinuationExpiredException(String message) {
        super(message);
    }
}
