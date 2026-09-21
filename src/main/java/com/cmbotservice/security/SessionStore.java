package com.cmbotservice.security;

import reactor.core.publisher.Mono;

/**
 * Read-only lookup of a BFF-issued session from the shared Redis store. This is the one seam the
 * exact (currently unconfirmed) Redis key format/serialization is isolated behind — selected via
 * {@code chatbot.security.redis.strategy}, same {@code @ConditionalOnProperty}-per-implementation
 * pattern already used for {@link com.cmbotservice.mlagent.MlAgentClient}. Adding a
 * differently-serialized strategy later needs a new implementation of this interface only; nothing
 * in {@link SessionAuthenticationWebFilter} changes.
 *
 * <p>Implementations must be strictly read-only: never write, refresh, or delete anything in the
 * session store — this service does not own that data.
 */
public interface SessionStore {

  /**
   * @param sessionCookieValue the raw value of the session cookie, never logged
   * @return the parsed session, or an empty {@code Mono} if no record exists for this value. Errors
   *     (e.g. Redis unreachable) are signalled as an error on the returned {@code Mono} — callers
   *     decide fail-open/fail-closed behavior.
   */
  Mono<SessionContext> findSession(String sessionCookieValue);
}
