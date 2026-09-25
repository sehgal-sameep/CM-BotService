package com.cmbotservice.context;

import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;

/**
 * Logs the start and end of every API request at INFO: {@code HTTP_REQUEST_RECEIVED} (method, path,
 * which identifying headers/cookies are present — never their credential values) and {@code
 * HTTP_REQUEST_COMPLETED} (status, duration, and whether it completed, failed, or the client
 * disconnected). For the SSE chat endpoint "completed" means the stream has ended, so the duration
 * is the whole stream's.
 *
 * <p>Runs right after {@link CorrelationIdFilter}, inside its Reactor context, so both lines carry
 * {@code corrId} in MDC. Infrastructure paths (actuator probes, Swagger UI assets, the OpenAPI
 * document) are skipped: a k8s prober hits health every few seconds, which would bury real traffic.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE + 1)
@Slf4j
public class RequestLoggingFilter implements WebFilter {

  private static final String SESSION_COOKIE = "SESSION";

  @Override
  public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
    ServerHttpRequest request = exchange.getRequest();
    String path = request.getPath().value();
    if (isInfrastructurePath(path)) {
      return chain.filter(exchange);
    }
    String method = request.getMethod().name();
    return Mono.defer(
        () -> {
          Instant start = Instant.now();
          log.info(
              "HTTP_REQUEST_RECEIVED method={} path={} origin={} tenantHeader={} orgHeader={}"
                  + " userHeader={} sessionCookiePresent={} remote={}",
              method,
              path,
              headerOrAbsent(request, HttpHeaders.ORIGIN),
              headerOrAbsent(request, RequestHeaders.TENANT_ID),
              headerOrAbsent(request, RequestHeaders.ORGANIZATION_ID),
              headerOrAbsent(request, RequestHeaders.USER_ID),
              request.getCookies().getFirst(SESSION_COOKIE) != null,
              request.getRemoteAddress() == null
                  ? "unknown"
                  : request.getRemoteAddress().getAddress().getHostAddress());
          return chain
              .filter(exchange)
              .doFinally(signal -> logCompletion(exchange, method, path, signal, start));
        });
  }

  private static void logCompletion(
      ServerWebExchange exchange, String method, String path, SignalType signal, Instant start) {
    HttpStatusCode status = exchange.getResponse().getStatusCode();
    int statusValue = status == null ? 200 : status.value();
    long durationMs = Duration.between(start, Instant.now()).toMillis();
    String outcome =
        switch (signal) {
          case CANCEL -> "client_disconnected";
          case ON_ERROR -> "failed";
          default -> "completed";
        };
    if (statusValue >= 500) {
      log.error(
          "HTTP_REQUEST_COMPLETED method={} path={} status={} outcome={} durationMs={}",
          method,
          path,
          statusValue,
          outcome,
          durationMs);
    } else if (statusValue >= 400) {
      log.warn(
          "HTTP_REQUEST_COMPLETED method={} path={} status={} outcome={} durationMs={}",
          method,
          path,
          statusValue,
          outcome,
          durationMs);
    } else {
      log.info(
          "HTTP_REQUEST_COMPLETED method={} path={} status={} outcome={} durationMs={}",
          method,
          path,
          statusValue,
          outcome,
          durationMs);
    }
  }

  /** Tenant/org/user ids are identifiers, not credentials — logged as-is to aid tracing. */
  private static String headerOrAbsent(ServerHttpRequest request, String name) {
    String value = request.getHeaders().getFirst(name);
    return value == null || value.isBlank() ? "<absent>" : value;
  }

  private static boolean isInfrastructurePath(String path) {
    return path.startsWith("/actuator")
        || path.startsWith("/swagger-ui")
        || path.startsWith("/v3/api-docs")
        || path.startsWith("/webjars")
        || path.equals("/favicon.ico");
  }
}
