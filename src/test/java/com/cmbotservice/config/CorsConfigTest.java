package com.cmbotservice.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.context.RequestHeaders;
import com.cmbotservice.security.SessionContext;
import com.cmbotservice.security.SessionStore;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;
import reactor.core.publisher.Mono;

/**
 * A browser on any origin must never see a CORS error, in either security mode: preflights are
 * answered before authentication runs, and every response — including an auth rejection — carries
 * the CORS headers, with credentials allowed and the caller's own origin echoed back.
 */
class CorsConfigTest {

  private static final String ANY_ORIGIN = "https://some-frontend.example.org:4200";

  private static void assertCorsAllowed(HttpHeaders headers) {
    assertThat(headers.getFirst(HttpHeaders.ACCESS_CONTROL_ALLOW_ORIGIN)).isEqualTo(ANY_ORIGIN);
    assertThat(headers.getFirst(HttpHeaders.ACCESS_CONTROL_ALLOW_CREDENTIALS)).isEqualTo("true");
  }

  private static RestTestClient.ResponseSpec preflight(RestTestClient client) {
    return client
        .method(HttpMethod.OPTIONS)
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(HttpHeaders.ORIGIN, ANY_ORIGIN)
        .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
        .header(
            HttpHeaders.ACCESS_CONTROL_REQUEST_HEADERS,
            "content-type,x-tenant-id,x-org-id,x-correlation-id")
        .exchange();
  }

  @Nested
  @SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
  class SecurityModeNone {

    @LocalServerPort private int port;
    private RestTestClient client;

    @BeforeEach
    void setUp() {
      client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void preflightFromAnyOrigin_isAllowed() {
      preflight(client)
          .expectStatus()
          .isOk()
          .expectHeader()
          .value(HttpHeaders.ACCESS_CONTROL_ALLOW_METHODS, v -> assertThat(v).contains("POST"))
          .expectHeader()
          .value(
              HttpHeaders.ACCESS_CONTROL_ALLOW_HEADERS,
              v -> assertThat(v.toLowerCase()).contains("x-tenant-id"))
          .expectBody()
          .consumeWith(result -> assertCorsAllowed(result.getResponseHeaders()));
    }

    @Test
    void privateNetworkPreflight_fromAPublicOriginToLocalhost_isGranted() {
      // Chrome's Private/Local Network Access: without this grant the browser blocks the
      // request and DevTools shows a CORS error with no response headers.
      client
          .method(HttpMethod.OPTIONS)
          .uri("/back-office-ai/api/v1/chat/messages")
          .header(HttpHeaders.ORIGIN, ANY_ORIGIN)
          .header(HttpHeaders.ACCESS_CONTROL_REQUEST_METHOD, "POST")
          .header("Access-Control-Request-Private-Network", "true")
          .exchange()
          .expectStatus()
          .isOk()
          .expectHeader()
          .valueEquals("Access-Control-Allow-Private-Network", "true")
          .expectBody()
          .consumeWith(result -> assertCorsAllowed(result.getResponseHeaders()));
    }

    @Test
    void actualRequestFromAnyOrigin_carriesCorsHeaders_andExposesCorrelationId() {
      client
          .post()
          .uri("/back-office-ai/api/v1/chat/messages")
          .header(HttpHeaders.ORIGIN, ANY_ORIGIN)
          .header(RequestHeaders.TENANT_ID, "tenant-1")
          .header(RequestHeaders.ORGANIZATION_ID, "org-1")
          .contentType(MediaType.APPLICATION_JSON)
          .accept(MediaType.TEXT_EVENT_STREAM)
          .body(Map.of("caseId", "case-1", "message", "trigger:empty"))
          .exchange()
          .expectStatus()
          .isOk()
          .expectHeader()
          .value(
              HttpHeaders.ACCESS_CONTROL_EXPOSE_HEADERS,
              v -> assertThat(v).containsIgnoringCase(RequestHeaders.CORRELATION_ID))
          .expectBody(String.class)
          .consumeWith(result -> assertCorsAllowed(result.getResponseHeaders()));
    }
  }

  @Nested
  @SpringBootTest(
      webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
      properties = {
        "chatbot.security.mode=BFF_SESSION",
        "chatbot.security.redis.strategy=test-stub"
      })
  @Import(SecurityModeBffSession.EmptySessionStoreConfig.class)
  class SecurityModeBffSession {

    @LocalServerPort private int port;
    private RestTestClient client;

    @BeforeEach
    void setUp() {
      client = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    }

    @Test
    void preflightWithoutASessionCookie_isAnsweredBeforeAuthentication_notRejectedWith401() {
      preflight(client)
          .expectStatus()
          .isOk()
          .expectBody()
          .consumeWith(result -> assertCorsAllowed(result.getResponseHeaders()));
    }

    @Test
    void authRejection_stillCarriesCorsHeaders_soTheFrontendSeesThe401() {
      client
          .post()
          .uri("/back-office-ai/api/v1/chat/messages")
          .header(HttpHeaders.ORIGIN, ANY_ORIGIN)
          .header(RequestHeaders.TENANT_ID, "tenant-1")
          .header(RequestHeaders.ORGANIZATION_ID, "org-1")
          .contentType(MediaType.APPLICATION_JSON)
          .body(Map.of("caseId", "case-1", "message", "hello"))
          .exchange()
          .expectStatus()
          .isUnauthorized()
          .expectBody()
          .consumeWith(result -> assertCorsAllowed(result.getResponseHeaders()));
    }

    @TestConfiguration
    static class EmptySessionStoreConfig {
      @Bean
      SessionStore emptySessionStore() {
        return new SessionStore() {
          @Override
          public Mono<SessionContext> findSession(String sessionCookieValue, String tenant) {
            return Mono.empty();
          }
        };
      }
    }
  }
}
