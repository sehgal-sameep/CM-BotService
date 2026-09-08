package com.cmbotservice.common;

/**
 * Stable machine-readable error codes used both in REST error bodies and in SSE
 * {@code error} events. Kept small and explicit rather than surfacing raw exception
 * messages to callers.
 */
public enum ErrorCode {
    VALIDATION_ERROR,
    NOT_FOUND,
    ML_AGENT_TIMEOUT,
    ML_AGENT_UNAVAILABLE,
    ML_AGENT_ERROR,
    CONCURRENCY_LIMIT_REACHED,
    /**
     * The ML Agent's {@code continuation}/{@code conversationId} was expired,
     * undecryptable, or tampered (its 4221/4222 codes) — the frontend must discard
     * whatever it was holding and start a new conversation. Distinct from
     * {@link #ML_AGENT_ERROR} because the remediation is different.
     */
    CONTINUATION_EXPIRED,
    INTERNAL_ERROR
}
