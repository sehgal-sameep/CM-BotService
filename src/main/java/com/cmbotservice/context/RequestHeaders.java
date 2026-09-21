package com.cmbotservice.context;

/**
 * HTTP header names used for request identity/tracing. Centralized so the eventual switch to real
 * authentication touches one place.
 */
public final class RequestHeaders {

  public static final String CORRELATION_ID = "X-Correlation-Id";

  /**
   * Placeholder for the authenticated analyst's identity. A POC-only stand-in for a real
   * authentication principal (JWT/session) — see {@link RequestContextResolver}.
   */
  public static final String USER_ID = "X-User-Id";

  /**
   * Tenant identifier, sent by the frontend as a header rather than a body field — both {@link
   * RequestContextResolver} implementations require it. Distinct from {@code
   * chatbot.security.session.tenant-header-name}, which is a separately configurable header used
   * only for the optional BFF-session tenant cross-check (see {@code
   * SessionAuthenticationWebFilter}) — they happen to share the same value today but serve
   * different purposes.
   */
  public static final String TENANT_ID = "X-Tenant-Id";

  /**
   * Organization identifier, sent by the frontend as a header. Forwarded to the ML Agent's {@code
   * AgentRequestContext.organization} — see {@link RequestContextResolver}.
   */
  public static final String ORGANIZATION_ID = "X-Org-Id";

  private RequestHeaders() {}
}
