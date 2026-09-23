package com.cmbotservice.security;

import com.cmbotservice.common.ErrorCode;
import org.springframework.http.HttpStatus;

/**
 * The exact set of rejection reasons the (currently minimal) authentication flow can produce — used
 * only in log lines, never in a response body (which carries a generic {@link ErrorCode} instead,
 * via {@link #errorCode()}).
 */
public enum AuthRejectionReason {
  MISSING_SESSION("missing_session", HttpStatus.UNAUTHORIZED, ErrorCode.UNAUTHENTICATED),
  MISSING_TENANT("missing_tenant", HttpStatus.UNAUTHORIZED, ErrorCode.UNAUTHENTICATED),
  SESSION_NOT_FOUND("session_not_found", HttpStatus.UNAUTHORIZED, ErrorCode.UNAUTHENTICATED),
  SESSION_STORE_UNAVAILABLE(
      "session_store_unavailable",
      HttpStatus.SERVICE_UNAVAILABLE,
      ErrorCode.SESSION_STORE_UNAVAILABLE);

  private final String logToken;
  private final HttpStatus httpStatus;
  private final ErrorCode errorCode;

  AuthRejectionReason(String logToken, HttpStatus httpStatus, ErrorCode errorCode) {
    this.logToken = logToken;
    this.httpStatus = httpStatus;
    this.errorCode = errorCode;
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
}
