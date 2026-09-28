package com.cmbotservice.security;

import com.cmbotservice.common.ErrorCode;
import org.springframework.http.HttpStatus;

/**
 * The exact set of rejection reasons the (currently minimal) authentication flow can produce. Each
 * one carries its log token (log lines only), the HTTP status, the {@link ErrorCode} returned in
 * the response body, and the client-facing message template.
 *
 * <p>Message templates take two positional arguments — {@code %1$s} the configured session cookie
 * name, {@code %2$s} the configured tenant header name — so they stay accurate if either is
 * reconfigured. Messages describe what the client must do; they never echo the cookie value, the
 * Redis key, or which of the not-found sub-causes (expired, wrong tenant, malformed record) applied
 * — that detail is in the server logs, keyed by correlation ID.
 */
public enum AuthRejectionReason {
  MISSING_SESSION(
      "missing_session",
      HttpStatus.UNAUTHORIZED,
      ErrorCode.SESSION_COOKIE_MISSING,
      "Authentication required. The request did not include a '%1$s' session cookie. Sign in to"
          + " start a session, and ensure the cookie is sent with the request (for cross-origin"
          + " calls, send requests with credentials included)."),
  MISSING_TENANT(
      "missing_tenant",
      HttpStatus.UNAUTHORIZED,
      ErrorCode.TENANT_HEADER_MISSING,
      "Authentication could not be completed. The '%2$s' header is required to identify the"
          + " tenant for this session but was missing or blank. Include the tenant identifier"
          + " and retry."),
  SESSION_NOT_FOUND(
      "session_not_found",
      HttpStatus.UNAUTHORIZED,
      ErrorCode.SESSION_INVALID_OR_EXPIRED,
      "Your session is invalid or has expired. No active session matches the provided '%1$s'"
          + " cookie for the tenant given in the '%2$s' header. Sign in again to continue."),
  SESSION_STORE_UNAVAILABLE(
      "session_store_unavailable",
      HttpStatus.SERVICE_UNAVAILABLE,
      ErrorCode.SESSION_STORE_UNAVAILABLE,
      "The authentication service is temporarily unavailable, so your session could not be"
          + " verified. Please try again shortly.");

  private final String logToken;
  private final HttpStatus httpStatus;
  private final ErrorCode errorCode;
  private final String messageTemplate;

  AuthRejectionReason(
      String logToken, HttpStatus httpStatus, ErrorCode errorCode, String messageTemplate) {
    this.logToken = logToken;
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
    this.messageTemplate = messageTemplate;
  }

  public String logToken() {
    return logToken;
  }

  public HttpStatus httpStatus() {
    return httpStatus;
  }

  public ErrorCode errorCode() {
    return errorCode;
  }

  /** The client-facing message, with the configured cookie and tenant header names filled in. */
  public String clientMessage(String cookieName, String tenantHeaderName) {
    return String.format(messageTemplate, cookieName, tenantHeaderName);
  }
}
