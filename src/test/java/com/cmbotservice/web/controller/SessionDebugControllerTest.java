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
import org.springframework.test.web.servlet.client.RestTestClient;
import reactor.core.publisher.Mono;

/**
 * Full-stack proof that the temporary {@link SessionDebugController} reuses {@link SessionStore}
 * correctly and is only registered in {@code chatbot.security.mode: BFF_SESSION}. A stub {@link
 * SessionStore} bean replaces the real Redis-backed one, same pattern as {@link
 * ChatControllerAuthenticationTest}.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {"chatbot.security.mode=BFF_SESSION", "chatbot.security.redis.strategy=test-stub"})
@Import(SessionDebugControllerTest.StubSessionStoreConfig.class)
class SessionDebugControllerTest {

  @LocalServerPort private int port;

  @Autowired private StubSessionStore stubSessionStore;

  private RestTestClient restTestClient;

  @BeforeEach
  void setUp() {
    restTestClient = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    stubSessionStore.reset();
  }

  @Test
  void sessionFound_returnsRawSessionFields() {
    stubSessionStore.setSession(
        new SessionContext(
            "alice", "tenant-1", "at-1", "rt-1", "{\"username\":\"alice\"}", "fp-1"));

    restTestClient
        .get()
        .uri("/api/v1/debug/session-lookup")
        .header("Cookie", "SESSION=abc123")
        .header("X-Tenant-Id", "tenant-1")
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody()
        .jsonPath("$.username")
        .isEqualTo("alice")
        .jsonPath("$.tenantId")
        .isEqualTo("tenant-1")
        .jsonPath("$.accessToken")
        .isEqualTo("at-1")
        .jsonPath("$.refreshToken")
        .isEqualTo("rt-1")
        .jsonPath("$.contextJson")
        .isEqualTo("{\"username\":\"alice\"}")
        .jsonPath("$.fingerprint")
        .isEqualTo("fp-1");
  }

  @Test
  void sessionNotFound_returns401Unauthenticated() {
    stubSessionStore.reset(); // no session set

    restTestClient
        .get()
        .uri("/api/v1/debug/session-lookup")
        .header("Cookie", "SESSION=missing")
        .header("X-Tenant-Id", "tenant-1")
        .exchange()
        .expectStatus()
        .isUnauthorized()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("UNAUTHENTICATED");
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
