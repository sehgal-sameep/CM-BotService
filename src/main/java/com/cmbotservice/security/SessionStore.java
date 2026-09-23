package com.cmbotservice.security;

import reactor.core.publisher.Mono;

/**
 * Read-only lookup of a BFF-issued session from the shared Redis store. Selected via {@code
 * chatbot.security.redis.strategy}, same {@code @ConditionalOnProperty}-per-implementation pattern
 * already used for {@link com.cmbotservice.mlagent.MlAgentClient}. Adding a differently-serialized
 * strategy later needs a new implementation of this interface only; nothing in {@link
 * SessionAuthenticationWebFilter} changes.
 *
 * <p>The single method here is shared by both the real authentication flow ({@link
 * SessionAuthenticationWebFilter}) and the temporary {@link
 * com.cmbotservice.web.controller.SessionDebugController} verification endpoint — deliberately, so
 * there is exactly one Redis lookup implementation to keep correct.
 *
 * <p>Implementations must be strictly read-only: never write, refresh, or delete anything in the
 * session store — this service does not own that data.
 */
public interface SessionStore {

  /**
   * @param sessionCookieValue the raw value of the session cookie, never logged
   * @param tenant the caller's tenant (from {@code chatbot.security.session.tenant-header-name}) —
   *     part of the session key itself
   * @return the parsed session, or an empty {@code Mono} if no record exists for this key. Errors
   *     (e.g. Redis unreachable) are signalled as an error on the returned {@code Mono} — callers
   *     decide fail-open/fail-closed behavior.
   */
  Mono<SessionContext> findSession(String sessionCookieValue, String tenant);
}
