package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentRejectedException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class MockMlAgentClientTest {

    private final MockMlAgentClient client = new MockMlAgentClient(5L, 5L);

    private MlAgentRequest request(String message) {
        return request(message, "conv-1", "cont-1");
    }

    private MlAgentRequest request(String message, String conversationId, String continuation) {
        return new MlAgentRequest("tenant-1", "case-1", continuation, conversationId, "msg-1", "analyst-1", null,
                "corr-1", "req-1", MlAgentRequest.SURFACE_CASE_MANAGER, true, message);
    }

    @Test
    void successScenario_emitsStartedThenTokensThenPayloadThenDone() {
        Flux<MlAgentStreamEvent> events = client.streamResponse(request("Summarize this case for me"));

        StepVerifier.create(events)
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .thenConsumeWhile(e -> e instanceof MlAgentStreamEvent.Token)
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Payload payload
                        && payload.payload().summary().keySignals().stream()
                                .allMatch(signal -> !signal.citations().isEmpty())
                        && CaseSummaryPayload.KNOWN_RESOLUTION_MARKS.contains(payload.payload().suggestedResolution().mark()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done done
                        && done.conversationId().equals("conv-1") && done.continuation().equals("cont-1"))
                .verifyComplete();
    }

    @Test
    void successScenario_tokenSequenceIsOneBasedAndIncreasing() {
        StepVerifier.create(client.streamResponse(request("Which rules were triggered?")))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token token
                        && token.sequence() == 1 && token.delta().contains("Two rules were triggered"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token token
                        && token.sequence() == 2 && token.delta().contains("velocity rule"))
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
    void continuationExpiredScenario_emitsStartedThenErrorsWithMlAgentContinuationExpiredException() {
        StepVerifier.create(client.streamResponse(request("trigger:continuation-expired")))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentContinuationExpiredException.class)
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

    @Test
    void noConversationIdOrContinuationInRequest_getsAFabricatedPairOnDone() {
        MlAgentStreamEvent.Done done = (MlAgentStreamEvent.Done) client
                .streamResponse(request("trigger:empty", null, null))
                .blockLast(Duration.ofSeconds(2));

        assertThat(done).isNotNull();
        assertThat(done.conversationId()).isNotBlank();
        assertThat(done.continuation()).isNotBlank();
    }

    @Test
    void conversationIdAndContinuationInRequest_areEchoedBackUnchangedOnDone() {
        String conversationId = "conversation-" + UUID.randomUUID();
        String continuation = "v1.mock." + UUID.randomUUID();

        MlAgentStreamEvent.Done done = (MlAgentStreamEvent.Done) client
                .streamResponse(request("trigger:empty", conversationId, continuation))
                .blockLast(Duration.ofSeconds(2));

        assertThat(done).isNotNull();
        assertThat(done.conversationId()).isEqualTo(conversationId);
        assertThat(done.continuation()).isEqualTo(continuation);
    }
}
