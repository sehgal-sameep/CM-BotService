package com.cmbotservice.service;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.LogSanitizer;
import com.cmbotservice.common.MlAgentCommunicationException;
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
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.sse.ServiceErrorEvent;
import com.cmbotservice.sse.SseEvents;
import com.cmbotservice.web.dto.ChatRequest;
import com.cmbotservice.web.dto.HistoryTurn;
import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadFullException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.reactor.bulkhead.operator.BulkheadOperator;
import io.github.resilience4j.reactor.circuitbreaker.operator.CircuitBreakerOperator;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

/**
 * Default {@link ChatOrchestrationService}: calls the ML Agent (behind the {@link MlAgentClient}
 * interface — never a concrete client type) through circuit breaker + bulkhead + timeout + retry,
 * and relays the streamed response as SSE. Fully non-blocking end to end — nothing here holds a
 * thread, sleeps, or buffers the full response; every layer of protection is a declarative {@code
 * Flux} operator, and cancellation (a client disconnect) tears the whole chain down automatically
 * via Reactor's own propagation.
 *
 * <p>A transparent proxy for the response contract: every ML Agent {@link AnswerEvent} is forwarded
 * to the frontend as-is (event name and payload exactly as the agent sent them — see {@link
 * SseEvents}), including the agent's own {@code error} event. The only event this service adds is
 * {@code service_error} ({@link ServiceErrorEvent}), for failures that never produced an agent
 * event to forward. Everything else here — resilience, logging, metrics — only observes the stream.
 *
 * <p>Stateless by design: nothing here is persisted or held in memory between requests. The real ML
 * Agent has no conversation/continuation identifier at all — resending the full {@code history} on
 * the next request is the caller's sole resumption mechanism, and this service never assembles,
 * stores, or replays that transcript itself.
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
      ChatProperties chatProperties) {
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
        .contextWrite(
            ctx ->
                ctx.put(MdcContext.TENANT_ID, context.tenantId())
                    .put(MdcContext.CASE_ID, context.caseId()));
  }

  private Flux<ServerSentEvent<Object>> doStreamMessage(
      RequestContext context, ChatRequest request) {
    String messageId = UUID.randomUUID().toString();
    // Identifiers and sizes only — the prompt and history are case free text that may
    // contain personal data, so their content is never logged (see LogSanitizer).
    log.info(
        "CHAT_REQUEST_RECEIVED messageId={} userId={} organization={} requestId={}"
            + " messageLength={} historyTurns={} endUserIdPresent={}",
        messageId,
        context.userId(),
        context.organization(),
        request.requestId() == null ? "<absent>" : request.requestId(),
        request.message().length(),
        request.history() == null ? 0 : request.history().size(),
        request.endUserId() != null);
    metrics.connectionOpened();

    boolean tooLong = request.message().length() > chatProperties.maxMessageLength();
    if (tooLong) {
      log.warn(
          "CHAT_REQUEST_REJECTED messageId={} reason=message too long messageLength={}"
              + " maxMessageLength={}",
          messageId,
          request.message().length(),
          chatProperties.maxMessageLength());
    }
    AtomicLong framesSent = new AtomicLong();
    Instant streamStart = Instant.now();

    Flux<AnswerEvent> mlEvents =
        tooLong
            ? Flux.error(
                new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "message exceeds the configured maximum length of "
                        + chatProperties.maxMessageLength()
                        + " characters"))
            : callMlAgent(context, request, messageId);

    return mlEvents
        .map(SseEvents::fromAnswerEvent)
        .onErrorResume(
            AbortedException.class,
            ex -> {
              log.info(
                  "SSE_CLIENT_ABORTED messageId={} — client connection closed mid-write",
                  messageId);
              return Flux.empty();
            })
        .onErrorResume(ex -> Flux.just(SseEvents.fromServiceError(toServiceError(messageId, ex))))
        .index()
        .map(indexed -> SseEvents.withId(indexed.getT1(), indexed.getT2()))
        .doOnNext(event -> framesSent.incrementAndGet())
        .doOnCancel(
            () -> {
              metrics.connectionCancelled();
              log.info(
                  "SSE_CLIENT_CANCELLED messageId={} framesSent={} durationMs={}",
                  messageId,
                  framesSent.get(),
                  Duration.between(streamStart, Instant.now()).toMillis());
            })
        .doOnComplete(
            () -> {
              metrics.connectionCompleted();
              log.info(
                  "SSE_STREAM_COMPLETED messageId={} framesSent={} durationMs={}",
                  messageId,
                  framesSent.get(),
                  Duration.between(streamStart, Instant.now()).toMillis());
            });
  }

  /**
   * The actual ML Agent call, wrapped in circuit breaker + bulkhead + timeout + retry. Each retry
   * re-subscribes this whole chain, so it correctly re-acquires a bulkhead permit and re-checks the
   * breaker on every attempt — a retried call is a fresh attempt, not a continuation.
   */
  private Flux<AnswerEvent> callMlAgent(
      RequestContext context, ChatRequest request, String messageId) {
    MlAgentRequest mlRequest =
        new MlAgentRequest(
            context.tenantId(),
            context.organization(),
            context.caseId(),
            toMlAgentHistory(request.history()),
            messageId,
            context.userId(),
            request.endUserId(),
            context.correlationId(),
            request.requestId(),
            request.message());

    AtomicBoolean firstEventSeen = new AtomicBoolean(false);
    AtomicBoolean agentErrorEventSeen = new AtomicBoolean(false);
    Map<AnswerEvent.EventCase, Integer> eventCounts =
        Collections.synchronizedMap(new EnumMap<>(AnswerEvent.EventCase.class));
    AtomicReference<Throwable> lastError = new AtomicReference<>();
    Instant startedAt = Instant.now();

    Flux<AnswerEvent> attempt =
        mlAgentClient
            .streamResponse(mlRequest)
            .doOnNext(
                evt -> {
                  if (firstEventSeen.compareAndSet(false, true)) {
                    Duration firstEventLatency = Duration.between(startedAt, Instant.now());
                    metrics.recordFirstResponseLatency(firstEventLatency);
                    log.info(
                        "ML_STREAM_STARTED messageId={} firstEvent={} firstEventLatencyMs={}",
                        messageId,
                        SseEvents.eventName(evt),
                        firstEventLatency.toMillis());
                  }
                  eventCounts.merge(evt.getEventCase(), 1, Integer::sum);
                  logAgentEvent(messageId, evt, agentErrorEventSeen);
                })
            .transformDeferred(CircuitBreakerOperator.of(circuitBreaker))
            .transformDeferred(BulkheadOperator.of(bulkhead))
            .timeout(
                Mono.delay(mlAgentProperties.firstResponseTimeout()),
                evt -> Mono.delay(mlAgentProperties.idleTimeout()))
            .onErrorMap(
                TimeoutException.class,
                e -> new MlAgentTimeoutException("The AI agent did not respond in time.", e));

    return Flux.defer(
            () -> {
              log.info(
                  "ML_REQUEST_STARTED messageId={} client={} caseId={} firstResponseTimeoutMs={}"
                      + " idleTimeoutMs={} maxStreamDurationMs={}",
                  messageId,
                  mlAgentClient.getClass().getSimpleName(),
                  context.caseId(),
                  mlAgentProperties.firstResponseTimeout().toMillis(),
                  mlAgentProperties.idleTimeout().toMillis(),
                  chatProperties.maxStreamDuration().toMillis());
              return attempt.retryWhen(buildRetrySpec(messageId, firstEventSeen));
            })
        .transform(flux -> withTotalDeadline(flux, chatProperties.maxStreamDuration()))
        .doOnError(lastError::set)
        .doOnError(this::recordFailureMetric)
        .doOnComplete(
            () -> {
              // The agent's own error event is forwarded as-is (never converted into an
              // exception), so a stream that carried one still completes normally — it is
              // only counted as a failure here, for observability.
              if (agentErrorEventSeen.get()) {
                metrics.mlRequestFailed();
              } else {
                metrics.mlRequestSucceeded();
              }
            })
        .doFinally(
            signalType -> {
              Duration elapsed = Duration.between(startedAt, Instant.now());
              metrics.recordStreamDuration(elapsed);
              logMlOutcome(messageId, signalType, lastError.get(), elapsed, eventCounts);
            });
  }

  private Retry buildRetrySpec(String messageId, AtomicBoolean firstEventSeen) {
    return Retry.backoff(retryProperties.maxAttempts(), retryProperties.initialBackoff())
        .maxBackoff(retryProperties.maxBackoff())
        .jitter(retryProperties.jitterFactor())
        .filter(ex -> isRetryable(ex) && !firstEventSeen.get())
        .doBeforeRetry(
            signal ->
                log.warn(
                    "ML_REQUEST_RETRY messageId={} retry={}/{} cause=[{}]",
                    messageId,
                    signal.totalRetries() + 1,
                    retryProperties.maxAttempts(),
                    LogSanitizer.causeChain(signal.failure())))
        // By default Reactor wraps the last failure in its own RetryExhaustedException
        // once attempts run out, which would break every instanceof-based
        // classification downstream (error mapping, metrics, logging). Propagate
        // the original cause directly instead.
        .onRetryExhaustedThrow((retrySpec, signal) -> signal.failure());
  }

  /**
   * Pass-through mapping only — {@code history} turns are forwarded exactly as the caller supplied
   * them, never inspected or reordered (see {@link MlAgentRequest}'s javadoc for why this doesn't
   * compromise statelessness).
   */
  private static List<MlAgentRequest.HistoryTurn> toMlAgentHistory(List<HistoryTurn> history) {
    if (history == null || history.isEmpty()) {
      return List.of();
    }
    return history.stream()
        .map(turn -> new MlAgentRequest.HistoryTurn(turn.role(), turn.content()))
        .toList();
  }

  private static boolean isRetryable(Throwable ex) {
    // Connection-level failures and "reached the agent but it said something went
    // wrong server-side" are retryable. Timeouts, malformed responses, explicit
    // rejections/refusals, and our own circuit breaker/bulkhead are not — see the
    // class Javadoc on each exception type for why.
    return ex instanceof MlAgentUnavailableException || ex instanceof MlAgentCommunicationException;
  }

  /**
   * Enforces an absolute ceiling on total stream duration, regardless of how active the stream is
   * (a per-element idle timeout alone would never trip if the ML Agent kept emitting chunks quickly
   * forever). {@code Flux#take(Duration)} truncates silently at the deadline as if the source had
   * completed normally, so we track whether the source actually reached a terminal signal before
   * that cut, and synthesize a timeout error if it didn't.
   */
  private static Flux<AnswerEvent> withTotalDeadline(
      Flux<AnswerEvent> source, Duration totalTimeout) {
    AtomicBoolean terminatedNaturally = new AtomicBoolean(false);
    return source
        .doOnComplete(() -> terminatedNaturally.set(true))
        .doOnError(e -> terminatedNaturally.set(true))
        .take(totalTimeout)
        .concatWith(
            Mono.defer(
                () ->
                    terminatedNaturally.get()
                        ? Mono.empty()
                        : Mono.error(
                            new MlAgentTimeoutException(
                                "ML Agent request exceeded the maximum total duration"))));
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

  /**
   * Every structurally meaningful agent event gets its own INFO line; {@code chunk} and {@code
   * ping} are high-volume, so they are only counted and reported in the completion line.
   */
  private static void logAgentEvent(
      String messageId, AnswerEvent evt, AtomicBoolean agentErrorEventSeen) {
    switch (evt.getEventCase()) {
      case TOOL_CALL ->
          log.info(
              "ML_EVENT_TOOL_CALL messageId={} toolCallId={} name={} argsJsonLength={}",
              messageId,
              evt.getToolCall().getToolCallId(),
              evt.getToolCall().getName(),
              evt.getToolCall().getArgsJson().length());
      case TOOL_RESULT ->
          log.info(
              "ML_EVENT_TOOL_RESULT messageId={} toolCallId={} status={} ms={} rowCount={}",
              messageId,
              evt.getToolResult().getToolCallId(),
              evt.getToolResult().getStatus(),
              evt.getToolResult().getMs(),
              evt.getToolResult().hasRowCount() ? evt.getToolResult().getRowCount() : "<unset>");
      case PAYLOAD ->
          log.info(
              "ML_EVENT_PAYLOAD messageId={} payloadType={} keySignals={} citations={}",
              messageId,
              evt.getPayload().getPayloadCase(),
              evt.getPayload().getCaseManagerAnswerPayload().getKeySignalsCount(),
              evt.getPayload().getCaseManagerAnswerPayload().getCitationsCount());
      case DONE ->
          log.info(
              "ML_EVENT_DONE messageId={} stopReason={} latencyMs={} tokensIn={} tokensOut={}",
              messageId,
              evt.getDone().getStopReason(),
              evt.getDone().getLatencyMs(),
              evt.getDone().getTokensIn(),
              evt.getDone().getTokensOut());
      case ERROR -> {
        agentErrorEventSeen.set(true);
        log.warn(
            "ML_EVENT_ERROR messageId={} code={} retryable={} — forwarded to the client as-is",
            messageId,
            evt.getError().getCode(),
            evt.getError().getRetryable());
      }
      case CHUNK, PING, EVENT_NOT_SET -> {
        // Counted only; see the ML_STREAM_COMPLETED line.
      }
    }
  }

  private static void logMlOutcome(
      String messageId,
      SignalType signalType,
      Throwable error,
      Duration elapsed,
      Map<AnswerEvent.EventCase, Integer> eventCounts) {
    long durationMs = elapsed.toMillis();
    String counts;
    synchronized (eventCounts) {
      counts = eventCounts.toString();
    }
    if (signalType == SignalType.ON_COMPLETE) {
      log.info(
          "ML_STREAM_COMPLETED messageId={} durationMs={} eventCounts={}",
          messageId,
          durationMs,
          counts);
    } else if (signalType == SignalType.CANCEL) {
      log.info(
          "ML_STREAM_CANCELLED messageId={} durationMs={} eventCounts={} — downstream cancelled"
              + " (client disconnected), ML Agent call cancelled",
          messageId,
          durationMs,
          counts);
    } else if (error instanceof MlAgentTimeoutException) {
      log.warn(
          "ML_REQUEST_TIMEOUT messageId={} durationMs={} eventCounts={} cause=[{}]",
          messageId,
          durationMs,
          counts,
          LogSanitizer.causeChain(error));
    } else if (error instanceof CallNotPermittedException) {
      log.warn(
          "CIRCUIT_BREAKER_OPEN messageId={} durationMs={} — ML Agent not called",
          messageId,
          durationMs);
    } else if (error instanceof BulkheadFullException) {
      log.warn(
          "CONCURRENCY_LIMIT_REACHED messageId={} durationMs={} — ML Agent not called",
          messageId,
          durationMs);
    } else if (error instanceof MlAgentRejectedException rejected) {
      log.warn(
          "ML_AGENT_REJECTED messageId={} durationMs={} errorCode={} cause=[{}]",
          messageId,
          durationMs,
          rejected.errorCode(),
          LogSanitizer.causeChain(error));
    } else if (error instanceof ResponseStatusException) {
      log.warn(
          "CHAT_REQUEST_INVALID messageId={} cause=[{}]",
          messageId,
          LogSanitizer.causeChain(error));
    } else if (error != null) {
      log.error(
          "ML_REQUEST_FAILED messageId={} durationMs={} eventCounts={} cause=[{}]",
          messageId,
          durationMs,
          counts,
          LogSanitizer.causeChain(error),
          error);
    }
  }

  /**
   * Only failures that produced no ML Agent event of their own end up here — transport errors,
   * timeouts, resilience rejections, validation. The agent's own {@code error} event never does.
   */
  private static ServiceErrorEvent toServiceError(String messageId, Throwable ex) {
    ErrorCode code;
    String message;
    if (ex instanceof MlAgentTimeoutException) {
      code = ErrorCode.ML_AGENT_TIMEOUT;
      message = "The AI agent did not respond in time.";
    } else if (ex instanceof MlAgentUnavailableException) {
      code = ErrorCode.ML_AGENT_UNAVAILABLE;
      message = "The AI agent could not be reached.";
    } else if (ex instanceof MlAgentRejectedException rejected) {
      code = rejected.errorCode();
      message = "The AI agent could not process this request.";
    } else if (ex instanceof MlAgentMalformedResponseException
        || ex instanceof MlAgentCommunicationException) {
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
    log.warn(
        "SSE_SERVICE_ERROR_SENT messageId={} errorCode={} errorMessage='{}'",
        messageId,
        code,
        message);
    return new ServiceErrorEvent(messageId, code, message, Instant.now());
  }
}
