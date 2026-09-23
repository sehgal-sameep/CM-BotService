package com.cmbotservice.web.controller;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.context.CorrelationIdFilter;
import com.cmbotservice.context.RequestHeaders;
import com.cmbotservice.security.SessionContext;
import com.cmbotservice.security.SessionStore;
import com.cmbotservice.web.ApiPaths;
import com.cmbotservice.web.dto.ErrorResponse;
import com.cmbotservice.web.dto.SessionDebugResponse;
import io.lettuce.core.RedisException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CookieValue;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

/**
 * <b>Temporary verification/debug endpoint.</b> Exercises exactly the same {@link SessionStore}
 * lookup {@code SessionAuthenticationWebFilter} uses for real request authentication, but returns
 * the raw session record instead of gating a business request — a fast way to confirm "session
 * cookie + tenant → Redis lookup → session data retrieved correctly" without going through the full
 * chat flow. Not part of this service's stable API surface; remove once the real authentication
 * flow no longer needs manual verification. Performs no validation beyond "was a record found" —
 * same deliberately minimal scope as {@code SessionAuthenticationWebFilter} today.
 *
 * <p>Only registered in {@code chatbot.security.mode: BFF_SESSION} (the same condition {@link
 * SessionStore} beans are created under).
 */
@RestController
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
@Tag(
    name = "Debug (temporary)",
    description =
        "TEMPORARY verification/debug endpoints — not part of this service's stable API, and not "
            + "intended to survive once the real authentication flow no longer needs manual "
            + "verification.")
public class SessionDebugController {

  private static final Logger log = LoggerFactory.getLogger(SessionDebugController.class);

  /** Matches {@code chatbot.security.session.cookie-name}'s default. */
  private static final String SESSION_COOKIE_NAME = "SESSION";

  private final SessionStore sessionStore;

  public SessionDebugController(SessionStore sessionStore) {
    this.sessionStore = sessionStore;
  }

  @GetMapping(ApiPaths.DEBUG_SESSION_LOOKUP)
  @Operation(
      summary = "[TEMPORARY] Look up a BFF session directly in Redis",
      description =
          """
                    TEMPORARY debug/verification endpoint — reuses the exact same read-only Redis \
                    lookup the real authentication flow (`SessionAuthenticationWebFilter`) uses, \
                    keyed on `session:<sessionId>:<tenant>`, and returns the raw record instead of \
                    gating a request. No fingerprint/CSRF/permission/expiry validation is performed \
                    here either — this only proves whether the lookup itself finds a record and \
                    what that record contains. Only available in `chatbot.security.mode: BFF_SESSION`.

                    In Swagger UI: paste the session cookie's raw value into the `SESSION` cookie \
                    field below (not the whole `Cookie:` header, just the value) and the caller's \
                    tenant into the `X-Tenant-Id` header field, then Execute.
                    """)
  @ApiResponse(
      responseCode = "200",
      description = "A session record was found at the key; returning it as stored.",
      content = @Content(schema = @Schema(implementation = SessionDebugResponse.class)))
  @ApiResponse(
      responseCode = "401",
      description = "No session record exists at session:<sessionId>:<tenant>.",
      content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
  @ApiResponse(
      responseCode = "503",
      description = "The shared Redis instance is unreachable.",
      content = @Content(schema = @Schema(implementation = ErrorResponse.class)))
  public Mono<ResponseEntity<?>> lookupSession(
      @Parameter(
              in = ParameterIn.COOKIE,
              name = SESSION_COOKIE_NAME,
              required = true,
              description = "The raw BFF session cookie value (not the whole Cookie header).")
          @CookieValue(SESSION_COOKIE_NAME)
          String sessionId,
      @Parameter(
              in = ParameterIn.HEADER,
              name = RequestHeaders.TENANT_ID,
              required = true,
              description =
                  "The caller's tenant — part of the Redis key, same as the real auth flow.")
          @RequestHeader(RequestHeaders.TENANT_ID)
          String tenant,
      ServerWebExchange exchange) {
    return sessionStore
        .findSession(sessionId, tenant)
        .<ResponseEntity<?>>map(session -> ResponseEntity.ok(toResponse(session)))
        .switchIfEmpty(Mono.fromSupplier(() -> notFound(exchange)))
        .onErrorResume(
            SessionDebugController::isRedisFailure,
            ex -> Mono.just(storeUnavailable(exchange, ex)));
  }

  private static SessionDebugResponse toResponse(SessionContext session) {
    return new SessionDebugResponse(
        session.username(),
        session.tenantId(),
        session.accessToken(),
        session.refreshToken(),
        session.contextJson(),
        session.fingerprint());
  }

  private static boolean isRedisFailure(Throwable ex) {
    return ex instanceof RedisConnectionFailureException || ex instanceof RedisException;
  }

  private static ResponseEntity<?> notFound(ServerWebExchange exchange) {
    log.warn("DEBUG_SESSION_LOOKUP no session record found");
    return ResponseEntity.status(401)
        .body(
            ErrorResponse.of(
                ErrorCode.UNAUTHENTICATED,
                "No session found in Redis for the given session cookie and tenant.",
                correlationId(exchange)));
  }

  private static ResponseEntity<?> storeUnavailable(ServerWebExchange exchange, Throwable ex) {
    log.error("DEBUG_SESSION_LOOKUP Redis unavailable: {}", ex.toString());
    return ResponseEntity.status(503)
        .body(
            ErrorResponse.of(
                ErrorCode.SESSION_STORE_UNAVAILABLE,
                "Redis is temporarily unavailable; please try again shortly.",
                correlationId(exchange)));
  }

  private static String correlationId(ServerWebExchange exchange) {
    return exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
  }
}
