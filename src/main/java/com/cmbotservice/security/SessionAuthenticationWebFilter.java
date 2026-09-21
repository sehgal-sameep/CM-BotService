package com.cmbotservice.security;

import com.cmbotservice.context.CorrelationIdFilter;
import com.cmbotservice.web.ApiPaths;
import com.cmbotservice.web.dto.ErrorResponse;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisException;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
 * Enforces the documented BFF-session authentication flow: extract session cookie → look up in the
 * shared Redis store (read-only) → check expiry → validate CSRF → cross-check tenant →
 * filter/authorize by {@code CHATBOT_}-prefixed permission. On success, stores a {@link
 * SessionContext} (and the filtered granted-authorities list) as exchange attributes for {@link
 * SessionRequestContextResolver} to consume; on rejection, writes this service's own {@link
 * ErrorResponse} JSON shape directly.
 *
 * <p>Runs only when {@code chatbot.security.mode: BFF_SESSION} — see {@link ChatbotSecurityMode}
 * for the master toggle. Ordered right after {@code CorrelationIdFilter} so a correlation ID is
 * always available for both the response header and rejection logging. Only actually gates {@link
 * ApiPaths#CHAT_MESSAGES} — every other path (actuator health/readiness, Swagger UI, OpenAPI JSON)
 * passes through untouched, since those have their own unauthenticated consumers (a k8s
 * liveness/readiness prober has no BFF session cookie to send).
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

  /** The filtered, {@code CHATBOT_}-prefixed subset of the session's permissions. */
  public static final String GRANTED_AUTHORITIES_ATTRIBUTE =
      SessionAuthenticationWebFilter.class.getName() + ".GRANTED_AUTHORITIES";

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
      return reject(exchange, AuthRejectionReason.MISSING_SESSION, null);
    }

    // NOTE: deliberately NOT `.flatMap(...).switchIfEmpty(...)` — continueWithSession(...)
    // returns Mono<Void>, which by definition never emits a value even on success (a
    // rejection or a successful chain.filter() delegation both "complete" with zero
    // elements), so switchIfEmpty chained after that flatMap would fire on *every*
    // request, not just an empty lookup, re-running the SESSION_NOT_FOUND rejection
    // after the response was already committed. Wrapping in Optional first keeps
    // "no session found" a real, distinguishable value the flatMap dispatches on.
    return sessionStore
        .findSession(sessionCookie.getValue())
        .map(Optional::of)
        .defaultIfEmpty(Optional.empty())
        .flatMap(
            maybeSession ->
                maybeSession
                    .map(session -> continueWithSession(exchange, chain, session))
                    .orElseGet(() -> reject(exchange, AuthRejectionReason.SESSION_NOT_FOUND, null)))
        .onErrorResume(
            SessionAuthenticationWebFilter::isRedisFailure,
            ex -> handleStoreError(exchange, chain, ex));
  }

  private static boolean isRedisFailure(Throwable ex) {
    return ex instanceof RedisConnectionFailureException || ex instanceof RedisException;
  }

  private Mono<Void> continueWithSession(
      ServerWebExchange exchange, WebFilterChain chain, SessionContext session) {
    if (session.isExpired()) {
      return reject(exchange, AuthRejectionReason.ACCESS_TOKEN_EXPIRED, session);
    }
    if (properties.csrf().enabled() && !csrfValid(exchange)) {
      return reject(exchange, AuthRejectionReason.CSRF_VALIDATION_FAILED, session);
    }
    if (!tenantValid(exchange, session)) {
      return reject(exchange, AuthRejectionReason.TENANT_MISMATCH, session);
    }
    List<String> grantedAuthorities = filterChatbotPermissions(session.permissions());
    if (grantedAuthorities.isEmpty()
        && !properties.authorization().permitWhenNoChatbotPermissions()) {
      return reject(exchange, AuthRejectionReason.NO_CHATBOT_PERMISSIONS, session);
    }

    log.debug(
        "AUTH_SUCCEEDED username={} tenantId={} grantedAuthorityCount={}",
        session.username(),
        session.tenantId(),
        grantedAuthorities.size());
    exchange.getAttributes().put(SessionContext.EXCHANGE_ATTRIBUTE, session);
    exchange.getAttributes().put(GRANTED_AUTHORITIES_ATTRIBUTE, grantedAuthorities);
    return chain.filter(exchange);
  }

  private boolean csrfValid(ServerWebExchange exchange) {
    HttpCookie csrfCookie =
        exchange.getRequest().getCookies().getFirst(properties.csrf().cookieName());
    String csrfHeader = exchange.getRequest().getHeaders().getFirst(properties.csrf().headerName());
    return csrfCookie != null
        && StringUtils.hasText(csrfCookie.getValue())
        && StringUtils.hasText(csrfHeader)
        && csrfCookie.getValue().equals(csrfHeader);
  }

  /**
   * The tenant header is an optional cross-check, not a required one: the flow only validates it
   * when the caller actually sends it — absent entirely is not a mismatch.
   */
  private boolean tenantValid(ServerWebExchange exchange, SessionContext session) {
    String tenantHeaderValue =
        exchange.getRequest().getHeaders().getFirst(properties.session().tenantHeaderName());
    if (!StringUtils.hasText(tenantHeaderValue)) {
      return true;
    }
    return tenantHeaderValue.equals(session.tenantId());
  }

  private List<String> filterChatbotPermissions(List<String> permissions) {
    if (permissions == null || permissions.isEmpty()) {
      return List.of();
    }
    String prefix = properties.authorization().requiredPermissionPrefix();
    return permissions.stream().filter(p -> p != null && p.startsWith(prefix)).toList();
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
    log.error(
        "AUTH_REJECTED reason={} cause={}",
        AuthRejectionReason.SESSION_STORE_UNAVAILABLE.logToken(),
        ex.toString());
    return writeRejection(exchange, AuthRejectionReason.SESSION_STORE_UNAVAILABLE);
  }

  private Mono<Void> reject(
      ServerWebExchange exchange, AuthRejectionReason reason, SessionContext session) {
    logRejection(exchange, reason, session);
    return writeRejection(exchange, reason);
  }

  private void logRejection(
      ServerWebExchange exchange, AuthRejectionReason reason, SessionContext session) {
    HttpCookie cookie =
        exchange.getRequest().getCookies().getFirst(properties.session().cookieName());
    boolean present = cookie != null && StringUtils.hasText(cookie.getValue());
    int length = present ? cookie.getValue().length() : 0;
    if (session != null) {
      log.warn(
          "AUTH_REJECTED reason={} cookiePresent={} cookieLength={} username={} tenantId={}",
          reason.logToken(),
          present,
          length,
          session.username(),
          session.tenantId());
    } else {
      log.warn(
          "AUTH_REJECTED reason={} cookiePresent={} cookieLength={}",
          reason.logToken(),
          present,
          length);
    }
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
      case MISSING_SESSION, SESSION_NOT_FOUND -> "Authentication required.";
      case ACCESS_TOKEN_EXPIRED -> "Your session has expired; please sign in again.";
      case CSRF_VALIDATION_FAILED -> "CSRF validation failed.";
      case TENANT_MISMATCH -> "Not permitted for this tenant.";
      case NO_CHATBOT_PERMISSIONS -> "You do not have permission to use this service.";
      case SESSION_STORE_UNAVAILABLE ->
          "Authentication service is temporarily unavailable; please try again shortly.";
    };
  }
}
