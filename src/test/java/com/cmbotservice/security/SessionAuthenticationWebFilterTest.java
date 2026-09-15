package com.cmbotservice.security;

import com.cmbotservice.web.dto.ErrorResponse;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpCookie;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link SessionAuthenticationWebFilter} directly — no Spring context needed —
 * against a stub {@link SessionStore}, same "no Spring context needed" style already
 * used by {@code ChatOrchestrationServiceTest}. Covers every rejection path the
 * documented flow specifies, plus the success path and both Redis-failure modes
 * (reject by default, fail-open when configured).
 */
class SessionAuthenticationWebFilterTest {

    private static final String COOKIE_NAME = "SESSION";
    private static final String CSRF_COOKIE_NAME = "XSRF-TOKEN";
    private static final String CSRF_HEADER_NAME = "X-XSRF-TOKEN";
    private static final String TENANT_HEADER_NAME = "X-Tenant-Id";

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static SecurityProperties properties(boolean csrfEnabled, boolean permitWhenNoPermissions, boolean failOpen) {
        return new SecurityProperties(
                ChatbotSecurityMode.BFF_SESSION,
                new SecurityProperties.Session(COOKIE_NAME, TENANT_HEADER_NAME),
                new SecurityProperties.Redis("json-blob", "ns", "localhost", 6379, false, "",
                        new SecurityProperties.Redis.FieldNames(
                                "username", "tenantId", "permissions", "organizations", "accessTokenExpiry", "fingerprint")),
                new SecurityProperties.Csrf(csrfEnabled, CSRF_COOKIE_NAME, CSRF_HEADER_NAME),
                new SecurityProperties.Authorization("CHATBOT_", permitWhenNoPermissions),
                new SecurityProperties.Cors(List.of()),
                failOpen);
    }

    private static SessionContext validSession() {
        return new SessionContext("alice", "tenant-1", List.of("CHATBOT_CHAT", "OTHER_PERM"),
                List.of("org-1"), Instant.now().plusSeconds(3600), "fp-123");
    }

    private static MockServerWebExchange exchangeWithSessionCookie(String cookieValue) {
        MockServerHttpRequest.BaseBuilder<?> builder = MockServerHttpRequest.post("/api/v1/chat/messages");
        if (cookieValue != null) {
            builder.cookie(new HttpCookie(COOKIE_NAME, cookieValue));
        }
        return MockServerWebExchange.from(builder.build());
    }

    @Test
    void nonChatPath_bypassesTheFilterEntirely_evenWithNoSessionCookie() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.error(new AssertionError("SessionStore must not be called for a non-chat path")),
                properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/actuator/health").build());
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isTrue();
    }

    @Test
    void missingCookie_rejectsWith401Unauthenticated() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                request -> Mono.error(new AssertionError("SessionStore must not be called")), properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie(null);
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
    }

    @Test
    void sessionNotFound_rejectsWith401Unauthenticated() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.empty(), properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
    }

    @Test
    void expiredAccessToken_rejectsWith401Unauthenticated() {
        SessionContext expired = new SessionContext("alice", "tenant-1", List.of("CHATBOT_CHAT"),
                List.of(), Instant.now().minusSeconds(60), null);
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(expired), properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");

        StepVerifier.create(filter.filter(exchange, ex -> Mono.empty())).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
        assertThat(errorCodeOf(exchange)).isEqualTo("UNAUTHENTICATED");
    }

    @Test
    void csrfHeaderMissing_rejectsWith403Forbidden() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(validSession()), properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/chat/messages")
                        .cookie(new HttpCookie(COOKIE_NAME, "abc123"), new HttpCookie(CSRF_COOKIE_NAME, "csrf-value"))
                        .build());

        StepVerifier.create(filter.filter(exchange, ex -> Mono.empty())).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(errorCodeOf(exchange)).isEqualTo("FORBIDDEN");
    }

    @Test
    void csrfCookieHeaderMismatch_rejectsWith403Forbidden() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(validSession()), properties(true, false, false), objectMapper);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/chat/messages")
                        .cookie(new HttpCookie(COOKIE_NAME, "abc123"), new HttpCookie(CSRF_COOKIE_NAME, "csrf-value"))
                        .header(CSRF_HEADER_NAME, "different-value")
                        .build());

        StepVerifier.create(filter.filter(exchange, ex -> Mono.empty())).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(errorCodeOf(exchange)).isEqualTo("FORBIDDEN");
    }

    @Test
    void csrfDisabled_skipsCsrfCheckEntirely() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(validSession()), properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123"); // no CSRF cookie/header at all
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isTrue();
    }

    @Test
    void tenantHeaderMismatch_rejectsWith403Forbidden() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(validSession()), properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.post("/api/v1/chat/messages")
                        .cookie(new HttpCookie(COOKIE_NAME, "abc123"))
                        .header(TENANT_HEADER_NAME, "some-other-tenant")
                        .build());

        StepVerifier.create(filter.filter(exchange, ex -> Mono.empty())).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(errorCodeOf(exchange)).isEqualTo("FORBIDDEN");
    }

    @Test
    void tenantHeaderAbsent_isNotValidatedAndRequestSucceeds() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(validSession()), properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123"); // no tenant header sent at all
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isTrue();
    }

    @Test
    void noChatbotPermissions_rejectsWith403Forbidden_whenNotPermissive() {
        SessionContext noChatbotPerms = new SessionContext("alice", "tenant-1", List.of("OTHER_PERM"),
                List.of(), Instant.now().plusSeconds(3600), null);
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(noChatbotPerms), properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");

        StepVerifier.create(filter.filter(exchange, ex -> Mono.empty())).verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.FORBIDDEN);
        assertThat(errorCodeOf(exchange)).isEqualTo("FORBIDDEN");
    }

    @Test
    void noChatbotPermissions_isPermitted_whenPermissiveFlagIsTrue() {
        SessionContext noChatbotPerms = new SessionContext("alice", "tenant-1", List.of("OTHER_PERM"),
                List.of(), Instant.now().plusSeconds(3600), null);
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(noChatbotPerms), properties(false, true, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isTrue();
    }

    @Test
    void validSession_passesThroughAndPopulatesExchangeAttributes() {
        SessionContext session = validSession();
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.just(session), properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isTrue();
        assertThat((SessionContext) exchange.getAttribute(SessionContext.EXCHANGE_ATTRIBUTE)).isEqualTo(session);
        @SuppressWarnings("unchecked")
        List<String> grantedAuthorities = exchange.getAttribute(SessionAuthenticationWebFilter.GRANTED_AUTHORITIES_ATTRIBUTE);
        assertThat(grantedAuthorities).containsExactly("CHATBOT_CHAT");
    }

    @Test
    void redisUnavailable_rejectsWith503_byDefault() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.error(new io.lettuce.core.RedisConnectionException("connection refused")),
                properties(false, false, false), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

        assertThat(chainInvoked).isFalse();
        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
        assertThat(errorCodeOf(exchange)).isEqualTo("SESSION_STORE_UNAVAILABLE");
    }

    @Test
    void redisUnavailable_failsOpen_whenConfigured() {
        SessionAuthenticationWebFilter filter = new SessionAuthenticationWebFilter(
                cookieValue -> Mono.error(new io.lettuce.core.RedisConnectionException("connection refused")),
                properties(false, false, true), objectMapper);
        MockServerWebExchange exchange = exchangeWithSessionCookie("abc123");
        AtomicBoolean chainInvoked = new AtomicBoolean();

        StepVerifier.create(filter.filter(exchange, ex -> {
            chainInvoked.set(true);
            return Mono.empty();
        })).verifyComplete();

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
