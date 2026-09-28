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
   * The request carried no session cookie, or the cookie was blank — see {@code
   * SessionAuthenticationWebFilter}. The caller has not signed in (or the cookie was not forwarded)
   * and should authenticate with the BFF before retrying.
   */
  SESSION_COOKIE_MISSING,
  /**
   * A session cookie was sent but the tenant header, which is part of the session key, was missing
   * or blank. This is a client integration defect rather than an expired login; fix the request
   * rather than re-authenticating.
   */
  TENANT_HEADER_MISSING,
  /**
   * Both the session cookie and tenant header arrived, but no usable session exists for that pair —
   * the session expired or was signed out, the cookie value is wrong, the tenant does not match the
   * session's tenant, or the stored record is malformed. The caller should re-authenticate with the
   * BFF; retrying this exact request will not help.
   */
  SESSION_INVALID_OR_EXPIRED,
  /**
   * A session was found and is still valid, but the request itself is rejected — CSRF mismatch,
   * tenant mismatch, or no {@code CHATBOT_}-prefixed permissions.
   */
  FORBIDDEN,
  /**
   * The shared Redis session store was unreachable, so no authentication decision could be made at
   * all. Distinct from {@link #SESSION_INVALID_OR_EXPIRED} because this is an infrastructure
   * outage, not a claim about the caller's identity.
   */
  SESSION_STORE_UNAVAILABLE,
  INTERNAL_ERROR
}
