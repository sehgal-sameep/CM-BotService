package com.cmbotservice.service;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.LogSanitizer;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentTimeoutException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.config.ChatProperties;
import com.cmbotservice.config.MlAgentProperties;
import com.cmbotservice.config.ResilienceProperties;
import com.cmbotservice.context.MdcContext;
import com.cmbotservice.context.RequestContext;
import com.cmbotservice.mlagent.MlAgentClient;
import com.cmbotservice.mlagent.MlAgentRequest;
import com.cmbotservice.mlagent.MlAgentStreamEvent;
import com.cmbotservice.sse.CaseSummaryEvent;
import com.cmbotservice.sse.ChatSseEvent;
import com.cmbotservice.sse.MessageChunkEvent;
import com.cmbotservice.sse.SseEvents;
import com.cmbotservice.sse.StreamCompleteEvent;
import com.cmbotservice.sse.StreamErrorEvent;
import com.cmbotservice.sse.StreamStartEvent;
import com.cmbotservice.web.dto.ChatRequest;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.netty.channel.AbortedException;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Default {@link ChatOrchestrationService}: calls the ML Agent (behind the
 * {@link MlAgentClient} interface — never a concrete client type) through circuit
 * breaker + bulkhead + timeout + retry, and relays the streamed response as SSE. Fully
 * non-blocking end to end — nothing here holds a thread, sleeps, or buffers the full
 * response; every layer of protection is a declarative {@code Flux} operator, and
 * cancellation (a client disconnect) tears the whole chain down automatically via
 * Reactor's own propagation.
 * <p>
 * Stateless by design: nothing here is persisted or held in memory between requests.
 * Conversation identity is a pure pass-through — the real ML Agent only reveals its
 * authoritative {@code conversationId}/{@code continuation} on
 * {@link MlAgentStreamEvent.Done}, streamed straight back to the caller (via
 * {@link StreamCompleteEvent}) and then forgotten.
 */
@Service
public class ChatOrchestrationServiceImpl implements ChatOrchestrationService {

    private static final Logger log = LoggerFactory.getLogger(ChatOrchestrationServiceImpl.class);

    private final MlAgentClient mlAgentClient;
    private final CircuitBreaker circuitBreaker;
    private final Bulkhead bulkhead;
    private final ChatMetrics metrics;
    private final MlAgentProperties mlAgentProperties;
    private final ResilienceProperties.Retry retryProperties;
    private final ChatProperties chatProperties;

    public ChatOrchestrationServiceImpl(
            MlAgentClient mlAgentClient,
            CircuitBreaker mlAgentCircuitBreaker,
            Bulkhead mlAgentBulkhead,
            ChatMetrics metrics,
            MlAgentProperties mlAgentProperties,
            ResilienceProperties resilienceProperties,
            ChatProperties chatProperties
    ) {
        this.mlAgentClient = mlAgentClient;
        this.circuitBreaker = mlAgentCircuitBreaker;
        this.bulkhead = mlAgentBulkhead;
        this.metrics = metrics;
        this.mlAgentProperties = mlAgentProperties;
        this.retryProperties = resilienceProperties.retry();
        this.chatProperties = chatProperties;
    }

    @Override
    public Flux<ServerSentEvent<Object>> streamMessage(RequestContext context, ChatRequest request) {
        return Flux.defer(() -> doStreamMessage(context, request))
                .contextWrite(ctx -> ctx
                        .put(MdcContext.TENANT_ID, context.tenantId())
                        .put(MdcContext.CASE_ID, context.caseId())
                        .put(MdcContext.CONVERSATION_ID, request.conversationId() == null ? "" : request.conversationId()));
    }

    private Flux<ServerSentEvent<Object>> doStreamMessage(RequestContext context, ChatRequest request) {
        String messageId = UUID.randomUUID().toString();
        log.info("CHAT_REQUEST_RECEIVED messageId={} promptPreview='{}'", messageId, LogSanitizer.preview(request.message()));
        metrics.connectionOpened();

        // conversationId/continuation start out as whatever the caller sent (echoed,
        // unconfirmed — see StreamStartEvent) and only become authoritative once the
        // ML Agent's Done event arrives, at which point these are overwritten.
        AtomicReference<String> conversationIdRef = new AtomicReference<>(request.conversationId());
        AtomicReference<String> continuationRef = new AtomicReference<>(request.continuation());
        AtomicInteger chunkCount = new AtomicInteger(0);

        Flux<MlAgentStreamEvent> mlEvents = request.message().length() > chatProperties.maxMessageLength()
                ? Flux.error(new ResponseStatusException(HttpStatus.BAD_REQUEST,
                        "message exceeds the configured maximum length of " + chatProperties.maxMessageLength() + " characters"))
                : callMlAgent(context, request, messageId);

        return mlEvents
                .doOnNext(event -> logIfStreamStarted(messageId, event))
                .map(event -> toChatSseEvent(event, messageId, chunkCount, conversationIdRef, continuationRef))
                .onErrorResume(AbortedException.class, ex -> {
                    log.debug("Client aborted mid-stream for messageId={}", messageId);
                    return Flux.empty();
                })
                .onErrorResume(ex -> Flux.just(toErrorEvent(messageId, conversationIdRef, ex)))
                .index()
                .map(indexed -> SseEvents.toServerSentEvent(indexed.getT1(), indexed.getT2()))
                .doOnCancel(() -> {
                    metrics.connectionCancelled();
                    log.info("SSE_CLIENT_CANCELLED messageId={}", messageId);
                })
                .doOnComplete(metrics::connectionCompleted);
    }

    /**
     * The actual ML Agent call, wrapped in circuit breaker + bulkhead + timeout +
     * retry. Each retry re-subscribes this whole chain, so it correctly re-acquires a
     * bulkhead permit and re-checks the breaker on every attempt — a retried call is a
     * fresh attempt, not a continuation.
     */
    private Flux<MlAgentStreamEvent> callMlAgent(RequestContext context, ChatRequest request, String messageId) {
        MlAgentRequest mlRequest = new MlAgentRequest(
                context.tenantId(), context.caseId(), request.continuation(), request.conversationId(), messageId,
                context.userId(), request.endUserId(), context.correlationId(), request.requestId(),
                MlAgentRequest.SURFACE_CASE_MANAGER, mlAgentProperties.includeResolutions(), request.message());

        AtomicBoolean firstEventSeen = new AtomicBoolean(false);
        AtomicReference<Throwable> lastError = new AtomicReference<>();
        Instant startedAt = Instant.now();

        Flux<MlAgentStreamEvent> attempt = mlAgentClient.streamResponse(mlRequest)
                .doOnNext(evt -> firstEventSeen.set(true))
                .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
                .transformDeferred(BulkheadOperator.of(bulkhead))
                .timeout(Mono.delay(mlAgentProperties.firstResponseTimeout()),
                        evt -> Mono.delay(mlAgentProperties.idleTimeout()))
                .onErrorMap(TimeoutException.class,
                        e -> new MlAgentTimeoutException("The AI agent did not respond in time.", e));

        return Flux.defer(() -> {
                    log.info("ML_REQUEST_STARTED messageId={}", messageId);
                    return attempt.retryWhen(buildRetrySpec(messageId, firstEventSeen));
                })
                .transform(flux -> withTotalDeadline(flux, chatProperties.maxStreamDuration()))
                .doOnError(lastError::set)
                .doOnError(this::recordFailureMetric)
                .doOnComplete(metrics::mlRequestSucceeded)
                .doFinally(signalType -> {
                    Duration elapsed = Duration.between(startedAt, Instant.now());
                    metrics.recordStreamDuration(elapsed);
                    logMlOutcome(messageId, signalType, lastError.get(), elapsed);
                });
    }

    private Retry buildRetrySpec(String messageId, AtomicBoolean firstEventSeen) {
        return Retry.backoff(retryProperties.maxAttempts(), retryProperties.initialBackoff())
                .maxBackoff(retryProperties.maxBackoff())
                .jitter(retryProperties.jitterFactor())
                .filter(ex -> isRetryable(ex) && !firstEventSeen.get())
                .doBeforeRetry(signal -> log.warn("ML_REQUEST_RETRY messageId={} attempt={} exceptionType={}",
                        messageId, signal.totalRetries() + 1, signal.failure().getClass().getSimpleName()))
                // By default Reactor wraps the last failure in its own RetryExhaustedException
                // once attempts run out, which would break every instanceof-based
                // classification downstream (error mapping, metrics, logging). Propagate
                // the original cause directly instead.
                .onRetryExhaustedThrow((retrySpec, signal) -> signal.failure());
    }

    private static boolean isRetryable(Throwable ex) {
        // Connection-level failures and "reached the agent but it said something went
        // wrong server-side" are retryable. Timeouts, malformed responses, explicit
        // rejections (401/403/404/422), expired continuations, and our own circuit
        // breaker/bulkhead are not — see the class Javadoc on each exception type for
        // why.
        return ex instanceof MlAgentUnavailableException || ex instanceof MlAgentCommunicationException;
    }

    /**
     * Enforces an absolute ceiling on total stream duration, regardless of how active
     * the stream is (a per-element idle timeout alone would never trip if the ML Agent
     * kept emitting chunks quickly forever). {@code Flux#take(Duration)} truncates
     * silently at the deadline as if the source had completed normally, so we track
     * whether the source actually reached a terminal signal before that cut, and
     * synthesize a timeout error if it didn't.
     */
    private static Flux<MlAgentStreamEvent> withTotalDeadline(Flux<MlAgentStreamEvent> source, Duration totalTimeout) {
        AtomicBoolean terminatedNaturally = new AtomicBoolean(false);
        return source
                .doOnComplete(() -> terminatedNaturally.set(true))
                .doOnError(e -> terminatedNaturally.set(true))
                .take(totalTimeout)
                .concatWith(Mono.defer(() -> terminatedNaturally.get()
                        ? Mono.empty()
                        : Mono.error(new MlAgentTimeoutException("ML Agent request exceeded the maximum total duration"))));
    }

    private void recordFailureMetric(Throwable ex) {
        if (ex instanceof MlAgentTimeoutException) {
            metrics.mlRequestTimedOut();
        } else if (ex instanceof BulkheadFullException || ex instanceof CallNotPermittedException) {
            metrics.mlRequestRejected();
        } else {
            metrics.mlRequestFailed();
        }
    }

    private static void logIfStreamStarted(String messageId, MlAgentStreamEvent event) {
        if (event instanceof MlAgentStreamEvent.Started) {
            log.info("ML_STREAM_STARTED messageId={}", messageId);
        }
    }

    private static void logMlOutcome(String messageId, SignalType signalType, Throwable error, Duration elapsed) {
        long durationMs = elapsed.toMillis();
        if (signalType == SignalType.ON_COMPLETE) {
            log.info("ML_STREAM_COMPLETED messageId={} durationMs={}", messageId, durationMs);
        } else if (error instanceof MlAgentTimeoutException) {
            log.warn("ML_REQUEST_TIMEOUT messageId={} durationMs={}", messageId, durationMs);
        } else if (error instanceof CallNotPermittedException) {
            log.warn("CIRCUIT_BREAKER_OPEN messageId={} durationMs={}", messageId, durationMs);
        } else if (error instanceof BulkheadFullException) {
            log.warn("CONCURRENCY_LIMIT_REACHED messageId={} durationMs={}", messageId, durationMs);
        } else if (error instanceof MlAgentContinuationExpiredException) {
            log.warn("CONTINUATION_EXPIRED messageId={} durationMs={}", messageId, durationMs);
        } else if (error instanceof MlAgentRejectedException rejected) {
            log.warn("ML_AGENT_REJECTED messageId={} durationMs={} errorCode={}", messageId, durationMs, rejected.errorCode());
        } else if (error != null) {
            log.error("ML_REQUEST_FAILED messageId={} durationMs={} exceptionType={}",
                    messageId, durationMs, error.getClass().getSimpleName());
        }
    }

    private static ChatSseEvent toChatSseEvent(MlAgentStreamEvent event, String messageId, AtomicInteger chunkCount,
                                                AtomicReference<String> conversationIdRef, AtomicReference<String> continuationRef) {
        return switch (event) {
            case MlAgentStreamEvent.Started ignored ->
                    new StreamStartEvent(conversationIdRef.get(), messageId, Instant.now());
            case MlAgentStreamEvent.Token token -> {
                chunkCount.incrementAndGet();
                yield new MessageChunkEvent(conversationIdRef.get(), messageId, token.sequence(), token.delta(), Instant.now());
            }
            case MlAgentStreamEvent.Payload payload ->
                    new CaseSummaryEvent(conversationIdRef.get(), messageId, payload.payload(), Instant.now());
            case MlAgentStreamEvent.Done done -> {
                conversationIdRef.set(done.conversationId());
                continuationRef.set(done.continuation());
                yield new StreamCompleteEvent(
                        done.conversationId(), done.continuation(), messageId, chunkCount.get(), Instant.now());
            }
        };
    }

    private static ChatSseEvent toErrorEvent(String messageId, AtomicReference<String> conversationIdRef, Throwable ex) {
        ErrorCode code;
        String message;
        if (ex instanceof MlAgentTimeoutException) {
            code = ErrorCode.ML_AGENT_TIMEOUT;
            message = "The AI agent did not respond in time.";
        } else if (ex instanceof MlAgentUnavailableException) {
            code = ErrorCode.ML_AGENT_UNAVAILABLE;
            message = "The AI agent could not be reached.";
        } else if (ex instanceof MlAgentContinuationExpiredException) {
            code = ErrorCode.CONTINUATION_EXPIRED;
            message = "Your conversation has expired; please start a new one.";
        } else if (ex instanceof MlAgentRejectedException rejected) {
            code = rejected.errorCode();
            message = "The AI agent could not process this request.";
        } else if (ex instanceof MlAgentMalformedResponseException || ex instanceof MlAgentCommunicationException) {
            code = ErrorCode.ML_AGENT_ERROR;
            message = "The AI agent failed to produce a response.";
        } else if (ex instanceof BulkheadFullException) {
            code = ErrorCode.CONCURRENCY_LIMIT_REACHED;
            message = "Too many concurrent AI requests; please try again shortly.";
        } else if (ex instanceof CallNotPermittedException) {
            code = ErrorCode.CONCURRENCY_LIMIT_REACHED;
            message = "The AI agent is temporarily unavailable; please try again shortly.";
        } else if (ex instanceof ResponseStatusException responseStatusException) {
            code = ErrorCode.VALIDATION_ERROR;
            message = responseStatusException.getReason();
        } else {
            code = ErrorCode.INTERNAL_ERROR;
            message = "An unexpected error occurred.";
        }
        return new StreamErrorEvent(conversationIdRef.get(), messageId, code, message, Instant.now());
    }
}
