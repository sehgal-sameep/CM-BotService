package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentRejectedException;
import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
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
  void successScenario_emitsStartedThenTokensThenPayloadThenDone() {
    Flux<MlAgentStreamEvent> events = client.streamResponse(request("Summarize this case for me"));

    StepVerifier.create(events)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .thenConsumeWhile(e -> e instanceof MlAgentStreamEvent.Token)
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Payload payload
                    && payload.payload().keySignals().stream()
                        .allMatch(signal -> !signal.citations().isEmpty()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done done && !done.truncated())
        .verifyComplete();
  }

  @Test
  void successScenario_tokenSequenceIsOneBasedAndIncreasing() {
    StepVerifier.create(client.streamResponse(request("Which rules were triggered?")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Token token
                    && token.sequence() == 1
                    && token.delta().contains("Two rules were triggered"))
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Token token
                    && token.sequence() == 2
                    && token.delta().contains("velocity rule"))
        .thenConsumeWhile(e -> e instanceof MlAgentStreamEvent.Token)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Payload)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done)
        .verifyComplete();
  }

  @Test
  void errorScenario_emitsStartedThenErrors() {
    StepVerifier.create(client.streamResponse(request("trigger:error please")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectError(MlAgentCommunicationException.class)
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void rejectedScenario_emitsStartedThenErrorsWithMlAgentRejectedException() {
    StepVerifier.create(client.streamResponse(request("trigger:rejected")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectError(MlAgentRejectedException.class)
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void emptyScenario_emitsStartedThenDoneWithNoTokensOrPayload() {
    StepVerifier.create(client.streamResponse(request("trigger:empty")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done)
        .verifyComplete();
  }

  @Test
  void timeoutScenario_emitsStartedThenHangsWithNoFurtherSignal() {
    // The mock has no timeout logic of its own anymore (ChatOrchestrationService's
    // own .timeout() operator is what ends this in production) — this just proves
    // it hangs cleanly after Started rather than completing/erroring on its own.
    StepVerifier.create(client.streamResponse(request("trigger:timeout")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNoEvent(Duration.ofMillis(50))
        .thenCancel()
        .verify(Duration.ofSeconds(2));
  }

  @Test
  void slowScenario_usesTheConfiguredSlowerDelayBetweenTokens() {
    long slowDelayMs = 10_000L;
    MockMlAgentClient slowClient = new MockMlAgentClient(5L, slowDelayMs);

    StepVerifier.withVirtualTime(() -> slowClient.streamResponse(request("trigger:slow")))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNoEvent(Duration.ofMillis(slowDelayMs - 100))
        .thenAwait(Duration.ofMillis(200))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token)
        .thenCancel()
        .verify(Duration.ofSeconds(2));
  }
}
