package com.cmbotservice.web.controller;

import com.cmbotservice.security.SessionContext;
import com.cmbotservice.security.SessionStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack proof that {@code chatbot.security.mode: BFF_SESSION} is actually wired
 * into the real request pipeline end to end — complementing {@link ChatControllerTest},
 * which runs against this repo's default {@code mode: NONE}. A stub {@link SessionStore}
 * bean replaces the real Redis-backed one (via {@code chatbot.security.redis.strategy}
 * set to a value {@code JsonBlobSessionStore}'s own condition won't match), so no real
 * Redis is needed to prove the wiring.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "chatbot.security.mode=BFF_SESSION",
        "chatbot.security.redis.strategy=test-stub"
})
@Import(ChatControllerAuthenticationTest.StubSessionStoreConfig.class)
class ChatControllerAuthenticationTest {

    private static final String SESSION_COOKIE = "SESSION";
    private static final String CSRF_COOKIE = "XSRF-TOKEN";
    private static final String CSRF_HEADER = "X-XSRF-TOKEN";
    private static final String CSRF_VALUE = "csrf-abc";

    @LocalServerPort
    private int port;

    @Autowired
    private StubSessionStore stubSessionStore;

    private RestTestClient restTestClient;

    @BeforeEach
    void setUp() {
        restTestClient = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
        stubSessionStore.reset();
    }

    @Test
    void missingSessionCookie_returns401Unauthenticated() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "hello"))
                .exchange()
                .expectStatus().isUnauthorized()
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("UNAUTHENTICATED");
    }

    @Test
    void csrfHeaderMismatch_returns403Forbidden() {
        stubSessionStore.setSession(validSession());

        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .header("Cookie", SESSION_COOKIE + "=abc123; " + CSRF_COOKIE + "=" + CSRF_VALUE)
                .header(CSRF_HEADER, "wrong-value")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "hello"))
                .exchange()
                .expectStatus().isForbidden()
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("FORBIDDEN");
    }

    @Test
    void validSessionAndCsrf_streamsSuccessfully() {
        stubSessionStore.setSession(validSession());

        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .header("Cookie", SESSION_COOKIE + "=abc123; " + CSRF_COOKIE + "=" + CSRF_VALUE)
                .header(CSRF_HEADER, CSRF_VALUE)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "trigger:empty"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> assertThat(body).contains("event:stream-start", "event:stream-complete"));
    }

    private static SessionContext validSession() {
        return new SessionContext("alice", "tenant-1", List.of("CHATBOT_CHAT"),
                List.of("org-1"), Instant.now().plusSeconds(3600), "fp-1");
    }

    @TestConfiguration
    static class StubSessionStoreConfig {
        @Bean
        StubSessionStore stubSessionStore() {
            return new StubSessionStore();
        }
    }

    static class StubSessionStore implements SessionStore {
        private volatile SessionContext session;

        void setSession(SessionContext session) {
            this.session = session;
        }

        void reset() {
            this.session = null;
        }

        @Override
        public Mono<SessionContext> findSession(String sessionCookieValue) {
            return session == null ? Mono.empty() : Mono.just(session);
        }
    }
}
