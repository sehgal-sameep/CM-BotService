package com.cmbotservice.web.controller;

import com.cmbotservice.context.RequestHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Full-stack (real Spring context, random port, real HTTP) tests of the one endpoint
 * this service exposes, using the default {@code MockMlAgentClient}. Verifies ordering
 * of the SSE contract, validation, ML-failure-as-error-event, and correlation ID
 * propagation — the behaviors a frontend integrator actually depends on.
 * <p>
 * Uses Spring Boot 4's {@code RestTestClient} (the unified successor to
 * {@code WebTestClient} for this purpose), bound directly to the random port the test
 * server started on — simpler and more portable across app types than relying on
 * Boot's own test-client auto-configuration to guess the right binding.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ChatControllerTest {

    @LocalServerPort
    private int port;

    private RestTestClient restTestClient;

    @BeforeEach
    void setUp() {
        restTestClient = RestTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @Test
    void successfulChatRequest_streamsEventsInOrderAndEchoesCorrelationId() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .header(RequestHeaders.CORRELATION_ID, "test-corr-abc")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "Summarize this case for me"))
                .exchange()
                .expectStatus().isOk()
                .expectHeader().valueEquals(RequestHeaders.CORRELATION_ID, "test-corr-abc")
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("event:stream-start", "event:message", "event:payload", "event:stream-complete");
                    int start = body.indexOf("event:stream-start");
                    int firstMessage = body.indexOf("event:message");
                    int payload = body.indexOf("event:payload");
                    int complete = body.indexOf("event:stream-complete");
                    assertThat(start).isLessThan(firstMessage);
                    assertThat(firstMessage).isLessThan(payload);
                    assertThat(payload).isLessThan(complete);
                });
    }

    @Test
    void continuationRoundTrips_acrossTwoFollowUpRequests() {
        String firstBody = restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "Summarize this case for me"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertThat(firstBody).isNotNull().contains("event:payload", "event:stream-complete");

        String conversationId = extractLastQuotedValue(firstBody, "conversationId");
        String continuation = extractLastQuotedValue(firstBody, "continuation");
        assertThat(conversationId).isNotBlank();
        assertThat(continuation).isNotBlank();

        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of(
                        "tenantId", "tenant-1", "caseId", "case-1",
                        "conversationId", conversationId, "continuation", continuation,
                        "message", "Which rules were triggered?"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("event:stream-complete");
                    assertThat(extractLastQuotedValue(body, "conversationId")).isEqualTo(conversationId);
                    assertThat(extractLastQuotedValue(body, "continuation")).isEqualTo(continuation);
                });
    }

    @Test
    void requestWithHistory_isAcceptedAndStreamsNormally() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of(
                        "tenantId", "tenant-1", "caseId", "case-1",
                        "history", java.util.List.of(
                                Map.of("role", "user", "content", "Summarize this case for me"),
                                Map.of("role", "assistant", "content", "Here's a summary...")),
                        "message", "Which rules were triggered?"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> assertThat(body).contains("event:stream-start", "event:stream-complete"));
    }

    private static String extractLastQuotedValue(String sseBody, String fieldName) {
        Matcher matcher = Pattern.compile("\"" + fieldName + "\":\"([^\"]*)\"").matcher(sseBody);
        String last = null;
        while (matcher.find()) {
            last = matcher.group(1);
        }
        return last;
    }

    @Test
    void blankMessage_returns400ValidationError() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", ""))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("VALIDATION_ERROR");
    }

    @Test
    void missingTenantId_returns400ValidationError() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("caseId", "case-1", "message", "hello"))
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody()
                .jsonPath("$.errorCode").isEqualTo("VALIDATION_ERROR");
    }

    @Test
    void mlAgentFailure_streamsAnErrorEventInsteadOfAnHttpErrorStatus() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "trigger:error"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> assertThat(body).contains("event:error", "ML_AGENT_ERROR"));
    }

    @Test
    void emptyResponseScenario_streamsStartThenCompleteWithNoChunks() {
        restTestClient.post()
                .uri("/api/v1/chat/messages")
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .body(Map.of("tenantId", "tenant-1", "caseId", "case-1", "message", "trigger:empty"))
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .value(body -> {
                    assertThat(body).contains("event:stream-start", "event:stream-complete");
                    assertThat(body).doesNotContain("event:message");
                });
    }

    @Test
    void unknownRoute_returns404() {
        restTestClient.post()
                .uri("/api/v1/chat/does-not-exist")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of())
                .exchange()
                .expectStatus().isNotFound();
    }
}
