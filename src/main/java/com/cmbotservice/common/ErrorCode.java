package com.cmbotservice.common;

/**
 * Stable machine-readable error codes used both in REST error bodies and in {@code service_error}
 * SSE events (failures originating in this backend or its transport — never the ML Agent's own
 * {@code error} event, whose {@code code} is forwarded untouched). Kept small and explicit rather
 * than surfacing raw exception messages to callers.
 */
public enum ErrorCode {
  VALIDATION_ERROR,
  NOT_FOUND,
  ML_AGENT_TIMEOUT,
  ML_AGENT_UNAVAILABLE,
  ML_AGENT_ERROR,
  CONCURRENCY_LIMIT_REACHED,
  /**
   * No valid BFF session could be established for this request (missing cookie, no matching Redis
   * record, or an expired access token) — see {@code SessionAuthenticationWebFilter}. The caller
   * should re-authenticate with the BFF; retrying this exact request will not help.
   */
  UNAUTHENTICATED,
  /**
   * A session was found and is still valid, but the request itself is rejected — CSRF mismatch,
   * tenant mismatch, or no {@code CHATBOT_}-prefixed permissions.
   */
  FORBIDDEN,
  /**
   * The shared Redis session store was unreachable, so no authentication decision could be made at
   * all. Distinct from {@link #UNAUTHENTICATED} because this is an infrastructure outage, not a
   * claim about the caller's identity.
   */
  SESSION_STORE_UNAVAILABLE,
  INTERNAL_ERROR
}
