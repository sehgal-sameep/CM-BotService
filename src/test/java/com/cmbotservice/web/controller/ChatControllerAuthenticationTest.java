package com.cmbotservice.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.context.RequestHeaders;
import com.cmbotservice.security.SessionContext;
import com.cmbotservice.security.SessionStore;
import java.util.Map;
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

/**
 * Full-stack proof that {@code chatbot.security.mode: BFF_SESSION} is actually wired into the real
 * request pipeline end to end — complementing {@link ChatControllerTest}, which runs against this
 * repo's default {@code mode: NONE}. A stub {@link SessionStore} bean replaces the real
 * Redis-backed one (via {@code chatbot.security.redis.strategy} set to a value {@code
 * JsonBlobSessionStore}'s own condition won't match), so no real Redis is needed to prove the
 * wiring. Covers the current, deliberately minimal flow only: a found session authenticates the
 * request outright, an absent one rejects with 401.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"chatbot.security.mode=BFF_SESSION", "chatbot.security.redis.strategy=test-stub"})
@Import(ChatControllerAuthenticationTest.StubSessionStoreConfig.class)
class ChatControllerAuthenticationTest {

  private static final String SESSION_COOKIE = "SESSION";

  @LocalServerPort private int port;

  @Autowired private StubSessionStore stubSessionStore;

  private RestTestClient restTestClient;

  @BeforeEach
  void setUp() {
    restTestClient = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    stubSessionStore.reset();
  }

  @Test
  void missingSessionCookie_returns401Unauthenticated() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "case-1", "message", "hello"))
        .exchange()
        .expectStatus()
        .isUnauthorized()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("UNAUTHENTICATED");
  }

  @Test
  void sessionNotFoundInRedis_returns401Unauthenticated() {
    stubSessionStore.reset(); // no session set

    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header("Cookie", SESSION_COOKIE + "=abc123")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "case-1", "message", "hello"))
        .exchange()
        .expectStatus()
        .isUnauthorized()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("UNAUTHENTICATED");
  }

  @Test
  void validSession_streamsSuccessfully() {
    stubSessionStore.setSession(validSession());

    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header("Cookie", SESSION_COOKIE + "=abc123")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "trigger:empty"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(body -> assertThat(body).contains("event:done"));
  }

  private static SessionContext validSession() {
    return new SessionContext(
        "alice", "tenant-1", "at-1", "rt-1", "{\"username\":\"alice\"}", "fp-1");
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
    public Mono<SessionContext> findSession(String sessionCookieValue, String tenant) {
      return session == null ? Mono.empty() : Mono.just(session);
    }
  }
}
