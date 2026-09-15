package com.cmbotservice.service;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.config.ChatProperties;
import com.cmbotservice.config.MlAgentProperties;
import com.cmbotservice.config.ResilienceProperties;
import com.cmbotservice.context.RequestContext;
import com.cmbotservice.mlagent.MlAgentClient;
import com.cmbotservice.mlagent.MlAgentRequest;
import com.cmbotservice.mlagent.MlAgentStreamEvent;
import com.cmbotservice.sse.StreamCompleteEvent;
import com.cmbotservice.sse.StreamErrorEvent;
import com.cmbotservice.web.dto.ChatRequest;
import com.cmbotservice.web.dto.HistoryTurn;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.util.unit.DataSize;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives {@link ChatOrchestrationServiceImpl} (via its {@link ChatOrchestrationService}
 * contract) directly against small, purpose-built resilience4j instances and stub
 * {@link MlAgentClient}s — no Spring context needed — to verify the resilience
 * composition (retry classification, circuit breaker, bulkhead, total-deadline)
 * without waiting on real-world timeouts or relying on {@code Thread.sleep} for
 * correctness.
 */
class ChatOrchestrationServiceTest {

    private final ChatMetrics metrics = new ChatMetrics(new SimpleMeterRegistry());

    private final MlAgentProperties mlAgentProperties = new MlAgentProperties(
            "mock", "localhost", 9090,
            Duration.ofMillis(300), Duration.ofMillis(300),
            DataSize.ofKilobytes(256), true);

    private ChatOrchestrationService newService(CircuitBreaker cb, Bulkhead bh, int maxRetryAttempts,
                                                 ChatProperties chatProperties, MlAgentClient client) {
        ResilienceProperties resilienceProperties = new ResilienceProperties(
                new ResilienceProperties.CircuitBreaker(50f, 10, Duration.ofSeconds(30), 2, 5),
                new ResilienceProperties.Bulkhead(50, Duration.ZERO),
                new ResilienceProperties.Retry(maxRetryAttempts, Duration.ofMillis(1), Duration.ofMillis(10), 0.1));
        return new ChatOrchestrationServiceImpl(client, cb, bh, metrics, mlAgentProperties, resilienceProperties, chatProperties);
    }

    private static RequestContext context() {
        return new RequestContext("tenant-1", "case-1", "analyst-1", "corr-1");
    }

    private static ChatRequest chatRequest(String message) {
        return new ChatRequest("tenant-1", "case-1", null, null, null, "req-1", null, message);
    }

    private static ChatProperties defaultChatProperties() {
        return new ChatProperties(4000, Duration.ofSeconds(10));
    }

    @Test
    void retriesBeforeFirstEvent_whenFailureIsTransientAndNothingHasStreamedYet() {
        AtomicInteger attempts = new AtomicInteger();
        // Flux.defer is essential here: streamResponse() is called exactly once per
        // logical request (see ChatOrchestrationServiceImpl#callMlAgent) — it's retryWhen
        // re-subscribing to the returned Flux that models a "retry", so the stub's
        // branching must be re-evaluated per subscription, not per call, to behave
        // differently on each attempt.
        MlAgentClient flakyThenSucceeds = request -> Flux.defer(() -> attempts.incrementAndGet() < 3
                ? Flux.error(new MlAgentUnavailableException("transient connection failure"))
                : Flux.just(new MlAgentStreamEvent.Started(), new MlAgentStreamEvent.Done("conv-1", "cont-1", 0, 0, 0)));
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t1"), Bulkhead.ofDefaults("t1"), 5, defaultChatProperties(), flakyThenSucceeds);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(attempts.get()).isEqualTo(3);
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOf(StreamCompleteEvent.class));
    }

    @Test
    void neverRetries_oncePartialContentHasAlreadyStreamed() {
        AtomicInteger attempts = new AtomicInteger();
        MlAgentClient emitsThenFails = request -> {
            attempts.incrementAndGet();
            return Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentUnavailableException("dropped mid-stream")));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t2"), Bulkhead.ofDefaults("t2"), 5, defaultChatProperties(), emitsThenFails);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(attempts.get()).isEqualTo(1);
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.ML_AGENT_UNAVAILABLE)));
    }

    @Test
    void malformedResponseFailure_isNeverRetried() {
        AtomicInteger attempts = new AtomicInteger();
        MlAgentClient alwaysMalformed = request -> {
            attempts.incrementAndGet();
            return Flux.error(new com.cmbotservice.common.MlAgentMalformedResponseException("bad payload"));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t3"), Bulkhead.ofDefaults("t3"), 5, defaultChatProperties(), alwaysMalformed);

        service.streamMessage(context(), chatRequest("hello")).collectList().block(Duration.ofSeconds(5));

        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    void circuitBreakerOpens_afterRepeatedFailures_thenRejectsWithoutCallingTheAgent() {
        AtomicInteger callCount = new AtomicInteger();
        // Flux.defer so the counter only increments on an actual subscription — once
        // the breaker is open, CircuitBreakerOperator rejects before ever subscribing
        // to this stub's Flux, so the count correctly stops climbing.
        MlAgentClient alwaysFails = request -> Flux.defer(() -> {
            callCount.incrementAndGet();
            return Flux.error(new MlAgentCommunicationException("boom"));
        });
        CircuitBreaker circuitBreaker = CircuitBreaker.of("cb-test", CircuitBreakerConfig.custom()
                .slidingWindowSize(2)
                .minimumNumberOfCalls(2)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofMinutes(1))
                .permittedNumberOfCallsInHalfOpenState(1)
                .build());
        Bulkhead bulkhead = Bulkhead.ofDefaults("bh-test");
        ChatOrchestrationService service = newService(circuitBreaker, bulkhead, 1, defaultChatProperties(), alwaysFails);

        for (int i = 0; i < 3; i++) {
            service.streamMessage(context(), chatRequest("hello")).collectList().block(Duration.ofSeconds(5));
        }
        assertThat(circuitBreaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);

        int callsBeforeRejection = callCount.get();
        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(callCount.get()).isEqualTo(callsBeforeRejection); // the agent was never actually called again
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.CONCURRENCY_LIMIT_REACHED)));
    }

    @Test
    void bulkheadRejects_whenMaxConcurrentCallsIsExceeded() {
        Bulkhead bulkhead = Bulkhead.of("bh-full", BulkheadConfig.custom()
                .maxConcurrentCalls(1)
                .maxWaitDuration(Duration.ZERO)
                .build());
        CircuitBreaker circuitBreaker = CircuitBreaker.ofDefaults("cb-passthrough");
        MlAgentClient holdsThePermitOpen = request ->
                Flux.concat(Mono.just(new MlAgentStreamEvent.Started()), Mono.never());
        ChatOrchestrationService service = newService(circuitBreaker, bulkhead, 1, defaultChatProperties(), holdsThePermitOpen);

        Disposable firstCallHoldingThePermit = service.streamMessage(context(), chatRequest("hello")).subscribe();
        try {
            List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                    .collectList().block(Duration.ofSeconds(5));

            assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                    StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.CONCURRENCY_LIMIT_REACHED)));
        } finally {
            firstCallHoldingThePermit.dispose();
        }
    }

    @Test
    void totalDeadline_stopsAStreamThatNeverCompletesEvenWhileActivelyEmitting() {
        MlAgentClient infiniteChunks = request -> Flux.concat(
                Mono.just(new MlAgentStreamEvent.Started()),
                Flux.interval(Duration.ofMillis(20))
                        .map(i -> new MlAgentStreamEvent.Token("chunk", i.intValue() + 1)));
        ChatProperties shortDeadline = new ChatProperties(4000, Duration.ofMillis(150));
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t4"), Bulkhead.ofDefaults("t4"), 1, shortDeadline, infiniteChunks);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(3));

        assertThat(events).isNotNull().isNotEmpty();
        assertThat(events.getLast().data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.ML_AGENT_TIMEOUT));
    }

    @Test
    void messageOverConfiguredMaxLength_isRejectedWithoutCallingTheAgent() {
        AtomicInteger callCount = new AtomicInteger();
        MlAgentClient shouldNeverBeCalled = request -> {
            callCount.incrementAndGet();
            return Flux.just(new MlAgentStreamEvent.Started(), new MlAgentStreamEvent.Done("conv-1", "cont-1", 0, 0, 0));
        };
        ChatProperties tinyLimit = new ChatProperties(5, Duration.ofSeconds(10));
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t5"), Bulkhead.ofDefaults("t5"), 1, tinyLimit, shouldNeverBeCalled);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("this message is too long"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(callCount.get()).isZero();
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.VALIDATION_ERROR)));
    }

    @Test
    void rejectedException_isNeverRetriedAndMapsToItsOwnErrorCode() {
        AtomicInteger attempts = new AtomicInteger();
        MlAgentClient alwaysRejected = request -> {
            attempts.incrementAndGet();
            return Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentRejectedException(ErrorCode.NOT_FOUND, "case not found in that tenant")));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t6"), Bulkhead.ofDefaults("t6"), 5, defaultChatProperties(), alwaysRejected);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(attempts.get()).isEqualTo(1);
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.NOT_FOUND)));
    }

    @Test
    void continuationExpiredException_isNeverRetriedAndMapsToContinuationExpired() {
        AtomicInteger attempts = new AtomicInteger();
        MlAgentClient alwaysExpired = request -> {
            attempts.incrementAndGet();
            return Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentContinuationExpiredException("continuation expired, start a new conversation")));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t7"), Bulkhead.ofDefaults("t7"), 5, defaultChatProperties(), alwaysExpired);

        List<ServerSentEvent<Object>> events = service.streamMessage(context(), chatRequest("hello"))
                .collectList().block(Duration.ofSeconds(5));

        assertThat(attempts.get()).isEqualTo(1);
        assertThat(events).isNotNull().anySatisfy(e -> assertThat(e.data()).isInstanceOfSatisfying(
                StreamErrorEvent.class, err -> assertThat(err.errorCode()).isEqualTo(ErrorCode.CONTINUATION_EXPIRED)));
    }

    @Test
    void history_isForwardedToTheMlAgentRequestUntouched_alongsideContinuationAndConversationId() {
        AtomicReference<MlAgentRequest> captured = new AtomicReference<>();
        MlAgentClient capturing = request -> {
            captured.set(request);
            return Flux.just(new MlAgentStreamEvent.Started(), new MlAgentStreamEvent.Done("conv-1", "cont-1", 0, 0, 0));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t8"), Bulkhead.ofDefaults("t8"), 5, defaultChatProperties(), capturing);
        ChatRequest request = new ChatRequest("tenant-1", "case-1", "conv-0", "cont-0",
                List.of(new HistoryTurn("user", "hi"), new HistoryTurn("assistant", "hello")),
                "req-1", null, "hello");

        service.streamMessage(context(), request).collectList().block(Duration.ofSeconds(5));

        assertThat(captured.get()).isNotNull();
        assertThat(captured.get().continuation()).isEqualTo("cont-0");
        assertThat(captured.get().conversationId()).isEqualTo("conv-0");
        assertThat(captured.get().history()).containsExactly(
                new MlAgentRequest.HistoryTurn("user", "hi"),
                new MlAgentRequest.HistoryTurn("assistant", "hello"));
    }

    @Test
    void missingHistory_isMappedToAnEmptyList_neverNull() {
        AtomicReference<MlAgentRequest> captured = new AtomicReference<>();
        MlAgentClient capturing = request -> {
            captured.set(request);
            return Flux.just(new MlAgentStreamEvent.Started(), new MlAgentStreamEvent.Done("conv-1", "cont-1", 0, 0, 0));
        };
        ChatOrchestrationService service = newService(
                CircuitBreaker.ofDefaults("t9"), Bulkhead.ofDefaults("t9"), 5, defaultChatProperties(), capturing);

        service.streamMessage(context(), chatRequest("hello")).collectList().block(Duration.ofSeconds(5));

        assertThat(captured.get().history()).isEmpty();
    }
}
