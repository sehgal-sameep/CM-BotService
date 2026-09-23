package com.cmbotservice.mlagent;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent.EventCase;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;

class MockMlAgentClientTest {

  private final MockMlAgentClient client = new MockMlAgentClient(5L, 5L);

  private MlAgentRequest request(String message) {
    return new MlAgentRequest(
        "tenant-1",
        "org-1",
        "case-1",
        List.of(),
        "msg-1",
        "analyst-1",
        null,
        "corr-1",
        "req-1",
        message);
  }

  @Test
  void successScenario_emitsTheRealContractsEvents_toolTraceThenChunksThenPayloadThenDone() {
    StepVerifier.create(client.streamResponse(request("Summarize this case for me")))
        .assertNext(
            e -> {
              assertThat(e.getEventCase()).isEqualTo(EventCase.TOOL_CALL);
              assertThat(e.getToolCall().getArgsJson()).contains("case-1");
            })
        .assertNext(
            e -> {
              assertThat(e.getEventCase()).isEqualTo(EventCase.TOOL_RESULT);
              assertThat(e.getToolResult().getStatus()).isEqualTo(ToolResult.Status.STATUS_OK);
            })
        .thenConsumeWhile(e -> e.getEventCase() == EventCase.CHUNK)
        .assertNext(
            e -> {
              assertThat(e.getEventCase()).isEqualTo(EventCase.PAYLOAD);
              assertThat(e.getPayload().getCaseManagerAnswerPayload().getKeySignalsList())
                  .allSatisfy(signal -> assertThat(signal.getCitationsList()).isNotEmpty());
            })
        .assertNext(e -> assertThat(e.getEventCase()).isEqualTo(EventCase.DONE))
        .verifyComplete();
  }

  @Test
  void successScenario_toolCallAndResultShareTheSameToolCallId() {
    List<AnswerEvent> events =
        client.streamResponse(request("hello")).collectList().block(Duration.ofSeconds(2));

    assertThat(events).isNotNull();
    assertThat(events.get(0).getToolCall().getToolCallId())
        .isEqualTo(events.get(1).getToolResult().getToolCallId());
  }

  @Test
  void successScenario_chunksCarryTheCannedAnswerText() {
    StepVerifier.create(client.streamResponse(request("Which rules were triggered?")))
        .expectNextCount(2) // tool_call, tool_result
        .assertNext(e -> assertThat(e.getChunk().getDelta()).contains("Two rules were triggered"))
        .assertNext(e -> assertThat(e.getChunk().getDelta()).contains("velocity rule"))
        .thenCancel()
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void errorScenario_failsTheFluxWithACommunicationException() {
    StepVerifier.create(client.streamResponse(request("trigger:error please")))
        .expectError(MlAgentCommunicationException.class)
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void rejectedScenario_failsTheFluxWithMlAgentRejectedException() {
    StepVerifier.create(client.streamResponse(request("trigger:rejected")))
        .expectError(MlAgentRejectedException.class)
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void agentErrorScenario_emitsTheAgentsOwnErrorEvent_ratherThanFailingTheFlux() {
    StepVerifier.create(client.streamResponse(request("trigger:agent-error")))
        .assertNext(
            e -> {
              assertThat(e.getEventCase()).isEqualTo(EventCase.ERROR);
              assertThat(e.getError().getCode()).isEqualTo(Error.Code.ERROR_CODE_DATA_UNAVAILABLE);
              assertThat(e.getError().getRetryable()).isTrue();
            })
        .verifyComplete();
  }

  @Test
  void emptyScenario_emitsOnlyDone() {
    StepVerifier.create(client.streamResponse(request("trigger:empty")))
        .assertNext(e -> assertThat(e.getEventCase()).isEqualTo(EventCase.DONE))
        .verifyComplete();
  }

  @Test
  void timeoutScenario_neverEmitsAnything() {
    // The mock has no timeout logic of its own (ChatOrchestrationService's .timeout()
    // operator is what ends this in production) — this just proves it hangs cleanly.
    StepVerifier.create(client.streamResponse(request("trigger:timeout")))
        .expectSubscription()
        .expectNoEvent(Duration.ofMillis(50))
        .thenCancel()
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void slowScenario_usesTheConfiguredSlowerDelayBetweenChunks() {
    long slowDelayMs = 10_000L;
    MockMlAgentClient slowClient = new MockMlAgentClient(5L, slowDelayMs);

    StepVerifier.withVirtualTime(() -> slowClient.streamResponse(request("trigger:slow")))
        .expectNextCount(2) // tool_call, tool_result arrive immediately
        .expectNoEvent(Duration.ofMillis(slowDelayMs - 100))
        .thenAwait(Duration.ofMillis(200))
        .assertNext(e -> assertThat(e.getEventCase()).isEqualTo(EventCase.CHUNK))
        .thenCancel()
        .verify(Duration.ofSeconds(2));
  }
}
