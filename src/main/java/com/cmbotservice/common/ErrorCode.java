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
    /**
     * No valid BFF session could be established for this request (missing cookie, no
     * matching Redis record, or an expired access token) — see
     * {@code SessionAuthenticationWebFilter}. The caller should re-authenticate with
     * the BFF; retrying this exact request will not help.
     */
    UNAUTHENTICATED,
    /**
     * A session was found and is still valid, but the request itself is rejected —
     * CSRF mismatch, tenant mismatch, or no {@code CHATBOT_}-prefixed permissions.
     */
    FORBIDDEN,
    /**
     * The shared Redis session store was unreachable, so no authentication decision
     * could be made at all. Distinct from {@link #UNAUTHENTICATED} because this is an
     * infrastructure outage, not a claim about the caller's identity.
     */
    SESSION_STORE_UNAVAILABLE,
    INTERNAL_ERROR
}
