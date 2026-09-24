package com.cmbotservice.security;

import com.cmbotservice.common.LogSanitizer;
import com.cmbotservice.context.CorrelationIdFilter;
import com.cmbotservice.web.ApiPaths;
import com.cmbotservice.web.dto.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.http.HttpCookie;
import org.springframework.http.MediaType;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

/**
 * Enforces a deliberately minimal BFF-session authentication flow: extract the session cookie +
 * tenant header → look up {@code session:<sessionId>:<tenant>} in Redis (read-only) → a record
 * found at all is treated as authenticated, full stop. There is currently no fingerprint, CSRF, or
 * permission check, and no token-expiry check — see {@link SessionContext} and {@link
 * JsonBlobSessionStore} for the (larger, diagrammed) validation flow this replaces; reintroduce
 * that logic here if/when that scope comes back rather than guessing at it piecemeal.
 *
 * <p>On success, the resolved {@link SessionContext} (carrying {@code accessToken}, for a future
 * call to the TFLabs Orchestrator Service) is stored as an exchange attribute for {@link
 * SessionRequestContextResolver} to consume; on rejection, writes this service's own {@link
 * ErrorResponse} JSON shape directly.
 *
 * <p>Runs only when {@code chatbot.security.mode: BFF_SESSION} — see {@link ChatbotSecurityMode}
 * for the master toggle. Ordered right after {@code CorrelationIdFilter} so a correlation ID is
 * always available for both the response header and rejection logging. Only actually gates {@link
 * ApiPaths#CHAT_MESSAGES} — every other path (actuator health/readiness, Swagger UI, OpenAPI JSON,
 * the temporary {@code SessionDebugController} endpoint) passes through untouched.
 *
 * <p><b>Why rejections are written directly instead of thrown:</b> a {@code WebFilter} runs
 * upstream of {@code DispatcherHandler}, so an exception thrown here never reaches
 * {@code @RestControllerAdvice} (that only intercepts exceptions raised during controller/handler
 * dispatch) — it would instead fall through to Spring Boot's generic default error page, a
 * different JSON shape than the {@link ErrorResponse} contract every other error path in this
 * service already uses. Writing the body here keeps that contract uniform.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class SessionAuthenticationWebFilter implements WebFilter {

  private static final Logger log = LoggerFactory.getLogger(SessionAuthenticationWebFilter.class);

  private final SessionStore sessionStore;
  private final SecurityProperties properties;
  private final ObjectMapper objectMapper;

  public SessionAuthenticationWebFilter(
      SessionStore sessionStore, SecurityProperties properties, ObjectMapper objectMapper) {
    this.sessionStore = sessionStore;
    this.properties = properties;
    this.objectMapper = objectMapper;
  }

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    // A WebFilter applies to every path by default — actuator health/readiness
    // probes, Swagger UI, etc. must stay reachable without a BFF session (a k8s
    // liveness/readiness prober has no browser session cookie to send), so only
    // the one real chat endpoint is actually gated here.
    if (!ApiPaths.CHAT_MESSAGES.equals(exchange.getRequest().getPath().value())) {
      return chain.filter(exchange);
    }

    HttpCookie sessionCookie =
        exchange.getRequest().getCookies().getFirst(properties.session().cookieName());
    if (sessionCookie == null || !StringUtils.hasText(sessionCookie.getValue())) {
      return reject(exchange, AuthRejectionReason.MISSING_SESSION);
    }

    // The tenant is part of the Redis session key itself (session:<sessionId>:<tenant>).
    String tenant =
        exchange.getRequest().getHeaders().getFirst(properties.session().tenantHeaderName());
    if (!StringUtils.hasText(tenant)) {
      return reject(exchange, AuthRejectionReason.MISSING_TENANT);
    }

    log.info(
        "AUTH_CHECK_STARTED session={} tenant={} — looking up the BFF session in Redis",
        LogSanitizer.maskSecret(sessionCookie.getValue()),
        tenant);

    // NOTE: deliberately NOT `.flatMap(...).switchIfEmpty(...)` — a successful
    // chain.filter() delegation is a Mono<Void>, which by definition never emits a
    // value, so switchIfEmpty chained after that flatMap would fire on *every*
    // request, not just an empty lookup. Wrapping in Optional first keeps "no
    // session found" a real, distinguishable value the flatMap dispatches on.
    return sessionStore
        .findSession(sessionCookie.getValue(), tenant)
        .map(Optional::of)
        .defaultIfEmpty(Optional.empty())
        .flatMap(
            maybeSession ->
                maybeSession
                    .map(session -> succeed(exchange, chain, session))
                    .orElseGet(() -> reject(exchange, AuthRejectionReason.SESSION_NOT_FOUND)))
        .onErrorResume(
            SessionAuthenticationWebFilter::isRedisFailure,
            ex -> handleStoreError(exchange, chain, ex));
  }

  private static boolean isRedisFailure(Throwable ex) {
    return ex instanceof RedisConnectionFailureException || ex instanceof RedisException;
  }

  private Mono<Void> succeed(
      ServerWebExchange exchange, WebFilterChain chain, SessionContext session) {
    log.info("AUTH_SUCCEEDED username={} tenantId={}", session.username(), session.tenantId());
    exchange.getAttributes().put(SessionContext.EXCHANGE_ATTRIBUTE, session);
    return chain.filter(exchange);
  }

  private Mono<Void> handleStoreError(
      ServerWebExchange exchange, WebFilterChain chain, Throwable ex) {
    if (properties.failOpenOnRedisError()) {
      log.warn(
          "AUTH_FAIL_OPEN reason={} — chatbot.security.fail-open-on-redis-error is true; permitting this "
              + "request WITHOUT a validated session. This must never be enabled in a shared/prod environment. cause={}",
          AuthRejectionReason.SESSION_STORE_UNAVAILABLE.logToken(),
          ex.toString());
      return chain.filter(exchange);
    }
    // The store has already logged REDIS_SESSION_LOOKUP_FAILED with the endpoint, full
    // cause chain, and stack trace; this line records the auth decision taken on it.
    log.error(
        "AUTH_REJECTED reason={} status={} cause={}",
        AuthRejectionReason.SESSION_STORE_UNAVAILABLE.logToken(),
        AuthRejectionReason.SESSION_STORE_UNAVAILABLE.httpStatus().value(),
        LogSanitizer.causeChain(ex));
    return writeRejection(exchange, AuthRejectionReason.SESSION_STORE_UNAVAILABLE);
  }

  private Mono<Void> reject(ServerWebExchange exchange, AuthRejectionReason reason) {
    HttpCookie cookie =
        exchange.getRequest().getCookies().getFirst(properties.session().cookieName());
    boolean present = cookie != null && StringUtils.hasText(cookie.getValue());
    log.warn(
        "AUTH_REJECTED reason={} status={} cookieName={} cookiePresent={} session={}",
        reason.logToken(),
        reason.httpStatus().value(),
        properties.session().cookieName(),
        present,
        present ? LogSanitizer.maskSecret(cookie.getValue()) : "<absent>");
    return writeRejection(exchange, reason);
  }

  private Mono<Void> writeRejection(ServerWebExchange exchange, AuthRejectionReason reason) {
    ServerHttpResponse response = exchange.getResponse();
    response.setStatusCode(reason.httpStatus());
    response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
    String correlationId = exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
    ErrorResponse body =
        ErrorResponse.of(reason.errorCode(), rejectionMessage(reason), correlationId);

    byte[] bytes;
    try {
      bytes = objectMapper.writeValueAsBytes(body);
    } catch (JsonProcessingException e) {
      log.error("Failed to serialize rejection body; falling back to a minimal error body", e);
      bytes = ("{\"errorCode\":\"" + reason.errorCode() + "\"}").getBytes(StandardCharsets.UTF_8);
    }
    DataBuffer buffer = response.bufferFactory().wrap(bytes);
    return response.writeWith(Mono.just(buffer));
  }

  private static String rejectionMessage(AuthRejectionReason reason) {
    return switch (reason) {
      case MISSING_SESSION, MISSING_TENANT, SESSION_NOT_FOUND -> "Authentication required.";
      case SESSION_STORE_UNAVAILABLE ->
          "Authentication service is temporarily unavailable; please try again shortly.";
    };
  }
}
