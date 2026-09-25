package com.cmbotservice.security;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.config.ApiProperties;
import com.cmbotservice.web.dto.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Drives {@link SessionAuthenticationWebFilter} directly — no Spring context needed — against a
 * stub {@link SessionStore}. Covers the (currently minimal) flow: missing cookie/tenant, session
 * not found, Redis unreachable (reject by default, fail-open when configured), and the success path
 * — a found record is authenticated outright, with no fingerprint/CSRF/permission/expiry check.
 */
class SessionAuthenticationWebFilterTest {

  private static final ApiProperties API = new ApiProperties("/back-office-ai");

  private static final String COOKIE_NAME = "SESSION";
  private static final String TENANT_HEADER_NAME = "X-Tenant-Id";
  private static final String TENANT = "tenant-1";

  // Registers JavaTimeModule to match RedisSessionConfig#securityObjectMapper — ErrorResponse
  // carries an Instant timestamp the plain default ObjectMapper can't serialize.
  private final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

  private static SecurityProperties properties(boolean failOpen) {
    return new SecurityProperties(
        ChatbotSecurityMode.BFF_SESSION,
        new SecurityProperties.Session(COOKIE_NAME, TENANT_HEADER_NAME),
        new SecurityProperties.Redis(
            "json-blob",
            "session",
            new SecurityProperties.Redis.FieldNames(
                "context_json", "access_token", "refresh_token", "fp")),
        new SecurityProperties.Cors(List.of()),
        failOpen);
  }

  private static SessionContext validSession() {
    return new SessionContext("alice", TENANT, "at-1", "rt-1", "{\"username\":\"alice\"}", "fp-1");
  }

  private static MockServerWebExchange exchangeWithSessionCookie(String cookieValue) {
    return exchangeWithSessionCookieAndTenant(cookieValue, TENANT);
  }

  private static MockServerWebExchange exchangeWithSessionCookieAndTenant(
      String cookieValue, String tenant) {
    MockServerHttpRequest.BaseBuilder<?> builder =
        MockServerHttpRequest.post("/back-office-ai/api/v1/chat/messages");
    if (cookieValue != null) {
      builder.cookie(new HttpCookie(COOKIE_NAME, cookieValue));
    }
    if (tenant != null) {
      builder.header(TENANT_HEADER_NAME, tenant);
    }
    return MockServerWebExchange.from(builder.build());
  }

  private static SessionStore store(Mono<SessionContext> sessionResult) {
    return (sessionCookieValue, tenant) -> sessionResult;
  }

  @Test
  void nonChatPath_bypassesTheFilterEntirely_evenWithNoSessionCookie() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.error(new AssertionError("SessionStore must not be called"))),
            properties(false),
            objectMapper,
            API);
    MockServerWebExchange exchange =
        MockServerWebExchange.from(MockServerHttpRequest.get("/actuator/health").build());
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isTrue();
  }

  @Test
  void missingCookie_rejectsWith401Unauthenticated() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.error(new AssertionError("SessionStore must not be called"))),
            properties(false),
            objectMapper,
            API);
    MockServerWebExchange exchange = exchangeWithSessionCookie(null);
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isFalse();
    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
  }

  @Test
  void missingTenantHeader_rejectsWith401Unauthenticated() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.error(new AssertionError("SessionStore must not be called"))),
            properties(false),
            objectMapper,
            API);
    MockServerWebExchange exchange = exchangeWithSessionCookieAndTenant("abc123", null);
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isFalse();
    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
  }

  @Test
  void sessionNotFound_rejectsWith401Unauthenticated() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.empty()), properties(false), objectMapper, API);
    MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isFalse();
    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
    assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
  }

  @Test
  void validSession_passesThroughAndPopulatesExchangeAttribute() {
    SessionContext session = validSession();
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.just(session)), properties(false), objectMapper, API);
    MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isTrue();
    assertThat((SessionContext) exchange.getAttribute(SessionContext.EXCHANGE_ATTRIBUTE))
        .isEqualTo(session);
  }

  @Test
  void redisUnavailable_rejectsWith503_byDefault() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.error(new io.lettuce.core.RedisConnectionException("connection refused"))),
            properties(false),
            objectMapper,
            API);
    MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isFalse();
    assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
    assertThat(errorCodeOf(exchange)).isEqualTo("SESSION_STORE_UNAVAILABLE");
  }

  @Test
  void redisUnavailable_failsOpen_whenConfigured() {
    SessionAuthenticationWebFilter filter =
        new SessionAuthenticationWebFilter(
            store(Mono.error(new io.lettuce.core.RedisConnectionException("connection refused"))),
            properties(true),
            objectMapper,
            API);
    MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
    AtomicBoolean chainInvoked = new AtomicBoolean();

    StepVerifier.create(
            filter.filter(
                exchange,
                ex -> {
                  chainInvoked.set(true);
                  return Mono.empty();
                }))
        .verifyComplete();

    assertThat(chainInvoked).isTrue();
    Object sessionAttribute = exchange.getAttribute(SessionContext.EXCHANGE_ATTRIBUTE);
    assertThat(sessionAttribute).isNull();
  }

  private String errorCodeOf(MockServerWebExchange exchange) {
    try {
      String json = exchange.getResponse().getBodyAsString().block();
      ErrorResponse body = objectMapper.readValue(json, ErrorResponse.class);
      return body.errorCode().name();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
