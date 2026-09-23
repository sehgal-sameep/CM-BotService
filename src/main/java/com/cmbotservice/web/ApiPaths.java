package com.cmbotservice.web;

/**
 * Shared path constants. There is a single stateless endpoint — {@code tenantId} and {@code caseId}
 * travel in the request body (see {@link com.cmbotservice.web.dto.ChatRequest}), not the URL, since
 * there is no backend-owned resource to nest a path under.
 */
public final class ApiPaths {

  public static final String CHAT_MESSAGES = "/api/v1/chat/messages";

  /**
   * Temporary verification/debug endpoint for the Redis session lookup used by {@code
   * chatbot.security.mode: BFF_SESSION} — see {@link
   * com.cmbotservice.web.controller.SessionDebugController}. Not part of this service's stable API
   * surface; remove once the real authentication flow no longer needs manual verification.
   */
  public static final String DEBUG_SESSION_LOOKUP = "/api/v1/debug/session-lookup";

  private ApiPaths() {}
}
