package com.cmbotservice.web.controller;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.context.RequestHeaders;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.client.RestTestClient;

/**
 * Full-stack (real Spring context, random port, real HTTP) tests of the one endpoint this service
 * exposes, using the default {@code MockMlAgentClient}. Verifies that the ML Agent's own events
 * reach the wire untranslated (event names, field names, nesting), their ordering, validation,
 * transport-failure-as-service_error-event, and correlation ID propagation — the behaviors a
 * frontend integrator actually depends on.
 *
 * <p>Uses Spring Boot 4's {@code RestTestClient} (the unified successor to {@code WebTestClient}
 * for this purpose), bound directly to the random port the test server started on — simpler and
 * more portable across app types than relying on Boot's own test-client auto-configuration to guess
 * the right binding.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ChatControllerTest {

  @LocalServerPort private int port;

  @Autowired private CircuitBreaker mlAgentCircuitBreaker;

  private RestTestClient restTestClient;

  @BeforeEach
  void setUp() {
    restTestClient = RestTestClient.bindToServer().baseUrl("http://localhost:" + port).build();
    // The breaker is a context-wide singleton: a failure-scenario test (each failed
    // attempt, retries included, counts toward it) must not open it for the next test.
    mlAgentCircuitBreaker.reset();
  }

  @Test
  void successfulChatRequest_streamsEventsInOrderAndEchoesCorrelationId() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.CORRELATION_ID, "test-corr-abc")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "Summarize this case for me"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectHeader()
        .valueEquals(RequestHeaders.CORRELATION_ID, "test-corr-abc")
        .expectBody(String.class)
        .value(
            body -> {
              assertThat(body)
                  .contains(
                      "event:tool_call",
                      "event:tool_result",
                      "event:chunk",
                      "event:payload",
                      "event:done");
              int toolCall = body.indexOf("event:tool_call");
              int toolResult = body.indexOf("event:tool_result");
              int firstChunk = body.indexOf("event:chunk");
              int payload = body.indexOf("event:payload");
              int done = body.indexOf("event:done");
              assertThat(toolCall).isLessThan(toolResult);
              assertThat(toolResult).isLessThan(firstChunk);
              assertThat(firstChunk).isLessThan(payload);
              assertThat(payload).isLessThan(done);
            });
  }

  @Test
  void responseBody_isTheAgentsOwnContract_withNoBackendRenamingOrWrapping() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "Summarize this case for me"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(
            body -> {
              // The agent's proto field names and nesting, verbatim.
              assertThat(body)
                  .contains(
                      "data:{\"tool_call\":{\"tool_call_id\":",
                      "\"args_json\":",
                      "data:{\"tool_result\":{\"tool_call_id\":",
                      "\"status\":\"STATUS_OK\"",
                      "\"row_count\":",
                      "data:{\"chunk\":{\"delta\":",
                      "data:{\"payload\":{\"case_manager_answer_payload\":{\"key_signals\":",
                      "data:{\"done\":{\"stop_reason\":\"STOP_REASON_COMPLETED\"",
                      "\"latency_ms\":",
                      "\"tokens_in\":",
                      "\"tokens_out\":");
              // None of the former backend-owned event names or field names.
              assertThat(body)
                  .doesNotContain(
                      "event:stream-start",
                      "event:stream-complete",
                      "event:message",
                      "event:tool-call",
                      "event:tool-result",
                      "messageId",
                      "toolCallId",
                      "argsJson",
                      "rowCount",
                      "keySignals",
                      "totalChunks",
                      "truncated",
                      "\"content\":",
                      "\"sequence\":");
            });
  }

  @Test
  void requestWithHistory_isAcceptedAndStreamsNormally() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(
            Map.of(
                "caseId", "case-1",
                "history",
                    java.util.List.of(
                        Map.of("role", "user", "content", "Summarize this case for me"),
                        Map.of("role", "assistant", "content", "Here's a summary...")),
                "message", "Which rules were triggered?"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(body -> assertThat(body).contains("event:chunk", "event:done"));
  }

  @Test
  void blankMessage_returns400ValidationError() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "case-1", "message", ""))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("VALIDATION_ERROR");
  }

  @Test
  void missingCaseId_isAccepted() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("message", "hello"))
        .exchange()
        .expectStatus()
        .isOk();
  }

  @Test
  void blankCaseId_isAccepted() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "  ", "message", "hello"))
        .exchange()
        .expectStatus()
        .isOk();
  }

  @Test
  void invalidCaseId_returns400ValidationError() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "bad id!", "message", "hello"))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("VALIDATION_ERROR");
  }

  @Test
  void missingTenantIdHeader_returns400ValidationError() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "case-1", "message", "hello"))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("VALIDATION_ERROR");
  }

  @Test
  void missingOrganizationIdHeader_returns400ValidationError() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of("caseId", "case-1", "message", "hello"))
        .exchange()
        .expectStatus()
        .isBadRequest()
        .expectBody()
        .jsonPath("$.errorCode")
        .isEqualTo("VALIDATION_ERROR");
  }

  @Test
  void transportFailure_streamsAServiceErrorEventInsteadOfAnHttpErrorStatus() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "trigger:error"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(
            body -> {
              assertThat(body).contains("event:service_error", "\"errorCode\":\"ML_AGENT_ERROR\"");
              assertThat(body).doesNotContain("event:error\n");
            });
  }

  @Test
  void agentErrorEvent_isForwardedUntouched_notReplacedByAServiceError() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "trigger:agent-error"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(
            body -> {
              assertThat(body)
                  .contains(
                      "event:error",
                      "data:{\"error\":{\"code\":\"ERROR_CODE_DATA_UNAVAILABLE\",\"retryable\":true}}");
              assertThat(body).doesNotContain("service_error", "errorCode");
            });
  }

  @Test
  void emptyResponseScenario_streamsOnlyDone() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/messages")
        .header(RequestHeaders.TENANT_ID, "tenant-1")
        .header(RequestHeaders.ORGANIZATION_ID, "org-1")
        .contentType(MediaType.APPLICATION_JSON)
        .accept(MediaType.TEXT_EVENT_STREAM)
        .body(Map.of("caseId", "case-1", "message", "trigger:empty"))
        .exchange()
        .expectStatus()
        .isOk()
        .expectBody(String.class)
        .value(
            body -> {
              assertThat(body).contains("event:done");
              assertThat(body).doesNotContain("event:chunk", "event:payload");
            });
  }

  @Test
  void unknownRoute_returns404() {
    restTestClient
        .post()
        .uri("/back-office-ai/api/v1/chat/does-not-exist")
        .contentType(MediaType.APPLICATION_JSON)
        .body(Map.of())
        .exchange()
        .expectStatus()
        .isNotFound();
  }
}
