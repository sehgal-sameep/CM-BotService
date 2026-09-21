package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.mlagent.grpc.v1.AgentRequestContext;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.AskCaseManagerRequest;
import com.cmbotservice.mlagent.grpc.v1.CaseManagerAnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.ChatAgentGrpc;
import com.cmbotservice.mlagent.grpc.v1.Chunk;
import com.cmbotservice.mlagent.grpc.v1.ConversationTurn;
import com.cmbotservice.mlagent.grpc.v1.Done;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.ToolCall;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

/**
 * Real {@link MlAgentClient} implementation: calls Thoughtful Labs' ML Agent over gRPC's
 * server-streaming {@code ChatAgent.AskCaseManager} RPC (see {@code
 * src/main/proto/chat_agent.proto}), translating its seven event types (<code>chunk</code>, <code>
 * tool_call</code>, <code>tool_result</code>, <code>payload</code>, <code>done</code>, <code>error
 * </code>, <code>ping</code>) into {@link MlAgentStreamEvent}. Never buffers the response — no
 * {@code collectList()}/{@code .block()}, just a straight {@code Flux}.
 *
 * <p>{@code tool_call}/{@code tool_result} are forwarded as {@link MlAgentStreamEvent.ToolCall}/
 * {@link MlAgentStreamEvent.ToolResult} domain events (also logged at debug, same as every other
 * event type) so the frontend can render the ML Agent's tool-use trace. {@code ping} is a pure
 * keepalive and remains logged only, never a domain event.
 *
 * <p>grpc-java's generated async stub is callback-based ({@code StreamObserver}), not {@code
 * Flux}-based — {@link #grpcEventFlux} bridges the two manually via {@code Flux.create} plus
 * grpc-java's own manual flow-control API ({@code disableAutoInboundFlowControl}/{@code
 * request(n)}), giving real backpressure without pulling in a third-party reactive-grpc codegen
 * plugin.
 *
 * <p>No client-side gRPC deadline is set here: {@code ChatOrchestrationService}'s existing
 * first-response/idle {@code .timeout()} operator is the one timeout authority, client-agnostic — a
 * second, competing deadline at this layer would just be a redundant knob.
 *
 * <p>Selected via {@code ml-agent.mode: grpc}; {@link MockMlAgentClient} steps aside automatically
 * ({@code @ConditionalOnProperty} on both, never a runtime check here).
 */
@Component
@ConditionalOnProperty(prefix = "ml-agent", name = "mode", havingValue = "grpc")
public class GrpcMlAgentClient implements MlAgentClient {

  private static final Logger log = LoggerFactory.getLogger(GrpcMlAgentClient.class);

  private final ChatAgentGrpc.ChatAgentStub chatAgentStub;

  public GrpcMlAgentClient(ChatAgentGrpc.ChatAgentStub chatAgentStub) {
    this.chatAgentStub = chatAgentStub;
  }

  @Override
  public Flux<MlAgentStreamEvent> streamResponse(MlAgentRequest request) {
    AtomicInteger lastSequence = new AtomicInteger(0);
    AskCaseManagerRequest protoRequest = toProtoRequest(request);

    Flux<MlAgentStreamEvent> events =
        grpcEventFlux(protoRequest)
            .<MlAgentStreamEvent>handle((event, sink) -> emit(event, lastSequence, sink))
            .onErrorMap(io.grpc.StatusRuntimeException.class, this::mapStatus);

    return Flux.concat(Mono.just(new MlAgentStreamEvent.Started()), events);
  }

  /**
   * Bridges grpc-java's callback-based async stub into a cold, backpressure-respecting {@code
   * Flux}: nothing is sent to the wire until subscribed ({@code stub.askCaseManager(...)} runs
   * inside the {@code Flux.create} lambda), downstream demand is translated into {@code
   * ClientCallStreamObserver#request(int)} calls, and cancellation (a client disconnect propagating
   * down from {@code ChatOrchestrationService}) calls {@code ClientCallStreamObserver#cancel(...)}
   * to actually stop the server-side call.
   */
  private Flux<AnswerEvent> grpcEventFlux(AskCaseManagerRequest protoRequest) {
    return Flux.create(
        sink -> {
          AtomicReference<ClientCallStreamObserver<AskCaseManagerRequest>> callStreamRef =
              new AtomicReference<>();
          ClientResponseObserver<AskCaseManagerRequest, AnswerEvent> observer =
              new ClientResponseObserver<>() {
                @Override
                public void beforeStart(
                    ClientCallStreamObserver<AskCaseManagerRequest> callStream) {
                  // Only disable auto flow control and stash the reference here — grpc-java
                  // forbids calling request()/cancel() before the call has actually started,
                  // and beforeStart() runs synchronously *before* start(). Wiring sink.onRequest
                  // here would invoke it immediately (Reactor requests unbounded demand as soon
                  // as it's registered), calling request() too early and failing with
                  // "IllegalStateException: Not started".
                  callStream.disableAutoInboundFlowControl();
                  callStreamRef.set(callStream);
                }

                @Override
                public void onNext(AnswerEvent event) {
                  sink.next(event);
                }

                @Override
                public void onError(Throwable t) {
                  sink.error(t);
                }

                @Override
                public void onCompleted() {
                  sink.complete();
                }
              };
          chatAgentStub.askCaseManager(protoRequest, observer);
          // chatAgentStub.askCaseManager(...) has now returned, meaning start() has already run
          // (grpc-java calls it synchronously as part of this method) — request()/cancel() are
          // safe from here on.
          ClientCallStreamObserver<AskCaseManagerRequest> callStream = callStreamRef.get();
          sink.onRequest(
              n -> callStream.request(n >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n));
          sink.onCancel(() -> callStream.cancel("Downstream cancelled", null));
        },
        FluxSink.OverflowStrategy.ERROR);
  }

  private void emit(
      AnswerEvent event, AtomicInteger lastSequence, SynchronousSink<MlAgentStreamEvent> sink) {
    switch (event.getEventCase()) {
      case CHUNK -> sink.next(toToken(event.getChunk(), lastSequence));
      case TOOL_CALL -> sink.next(toToolCall(event.getToolCall()));
      case TOOL_RESULT -> sink.next(toToolResult(event.getToolResult()));
      case PAYLOAD -> sink.next(toPayload(event.getPayload()));
      case DONE -> sink.next(toDone(event.getDone()));
      case ERROR -> sink.error(toErrorException(event.getError()));
      case PING -> log.trace("ML Agent ping received");
      // Per the contract: "Clients MUST ignore events whose `event` oneof is
      // unset or unrecognised and continue reading the stream" — no error, no
      // domain event, just skip it.
      case EVENT_NOT_SET ->
          log.debug("ML Agent AnswerEvent had no event set; ignoring per contract");
    }
  }

  private static MlAgentStreamEvent.Token toToken(Chunk chunk, AtomicInteger lastSequence) {
    return new MlAgentStreamEvent.Token(chunk.getDelta(), lastSequence.incrementAndGet());
  }

  private static MlAgentStreamEvent.ToolCall toToolCall(ToolCall toolCall) {
    log.debug("ML Agent tool_call id={} name={}", toolCall.getToolCallId(), toolCall.getName());
    return new MlAgentStreamEvent.ToolCall(
        toolCall.getToolCallId(), toolCall.getName(), toolCall.getArgsJson());
  }

  private static MlAgentStreamEvent.ToolResult toToolResult(ToolResult toolResult) {
    log.debug(
        "ML Agent tool_result id={} status={} ms={} rowCount={}",
        toolResult.getToolCallId(),
        toolResult.getStatus(),
        toolResult.getMs(),
        toolResult.hasRowCount() ? toolResult.getRowCount() : "n/a");
    MlAgentStreamEvent.ToolResult.Status status =
        toolResult.getStatus() == ToolResult.Status.STATUS_OK
            ? MlAgentStreamEvent.ToolResult.Status.OK
            : MlAgentStreamEvent.ToolResult.Status.FAILED;
    Long rowCount = toolResult.hasRowCount() ? toolResult.getRowCount() : null;
    return new MlAgentStreamEvent.ToolResult(
        toolResult.getToolCallId(), status, toolResult.getMs(), rowCount);
  }

  private static MlAgentStreamEvent.Payload toPayload(AnswerPayload proto) {
    if (proto.getPayloadCase() != AnswerPayload.PayloadCase.CASE_MANAGER_ANSWER_PAYLOAD) {
      throw new MlAgentMalformedResponseException(
          "ML Agent 'payload' event has no recognised payload set");
    }
    CaseSummaryPayload payload = toDomainPayload(proto.getCaseManagerAnswerPayload());
    CaseSummaryPayloadValidator.validate(payload);
    return new MlAgentStreamEvent.Payload(payload);
  }

  private static MlAgentStreamEvent.Done toDone(Done done) {
    return new MlAgentStreamEvent.Done(
        done.getLatencyMs(),
        done.getTokensIn(),
        done.getTokensOut(),
        done.getStopReason() == Done.StopReason.STOP_REASON_TRUNCATED);
  }

  /**
   * Unlike the old contract's {@code error} event, this one carries no human-readable message field
   * at all ("Debug detail is in server logs under request_id; nothing else travels here") — the
   * message text here is this backend's own, chosen from the code, not anything the agent sent.
   */
  private static Throwable toErrorException(Error error) {
    String message = messageFor(error.getCode());
    // The contract now carries retryability explicitly as a boolean rather than
    // implying it from the error code — this reuses ChatOrchestrationServiceImpl's
    // existing type-based retry classification (MlAgentCommunicationException is
    // retried, MlAgentRejectedException never is) instead of duplicating that
    // decision here.
    if (error.getRetryable()) {
      return new MlAgentCommunicationException(message);
    }
    return new MlAgentRejectedException(mapErrorCode(error.getCode()), message);
  }

  private static ErrorCode mapErrorCode(Error.Code code) {
    return switch (code) {
      case ERROR_CODE_MODEL_REFUSED -> ErrorCode.ML_AGENT_REFUSED;
      case ERROR_CODE_DATA_UNAVAILABLE -> ErrorCode.ML_AGENT_ERROR;
      // ERROR_CODE_INTERNAL, ERROR_CODE_UNSPECIFIED, and any future/unrecognised
      // value all fall back to a plain internal error — "Clients MUST treat any
      // unrecognised value as ERROR_CODE_INTERNAL" per the contract.
      default -> ErrorCode.INTERNAL_ERROR;
    };
  }

  private static String messageFor(Error.Code code) {
    return switch (code) {
      case ERROR_CODE_MODEL_REFUSED -> "The ML Agent refused to process the request";
      case ERROR_CODE_DATA_UNAVAILABLE -> "The ML Agent's data source was unavailable";
      default -> "The ML Agent reported an internal error";
    };
  }

  /**
   * gRPC-level rejections/failures — the direct analog of the old HTTP integration's pre-stream
   * 401/403/404/422/429/503 status mapping, just keyed off {@link Status.Code} instead of an HTTP
   * status.
   */
  private Throwable mapStatus(io.grpc.StatusRuntimeException ex) {
    Status.Code code = ex.getStatus().getCode();
    return switch (code) {
      case UNAVAILABLE, UNKNOWN ->
          new MlAgentUnavailableException("Could not reach the ML Agent", ex);
      case RESOURCE_EXHAUSTED ->
          new MlAgentCommunicationException(
              "ML Agent is temporarily unavailable (status " + code + ")", ex);
      case UNAUTHENTICATED, PERMISSION_DENIED ->
          new MlAgentRejectedException(
              ErrorCode.INTERNAL_ERROR, "The ML Agent rejected our credentials or tenant access");
      case NOT_FOUND ->
          new MlAgentRejectedException(ErrorCode.NOT_FOUND, "Case not found in that tenant");
      case INVALID_ARGUMENT ->
          new MlAgentRejectedException(
              ErrorCode.VALIDATION_ERROR, "Malformed request or missing required context");
      default ->
          new MlAgentCommunicationException("ML Agent returned an unexpected status " + code, ex);
    };
  }

  private static AskCaseManagerRequest toProtoRequest(MlAgentRequest request) {
    AgentRequestContext requestContext =
        AgentRequestContext.newBuilder()
            .setTenant(request.tenantId())
            .setOrganization(request.organization() == null ? "" : request.organization())
            .setAgentSessionId(request.requestId() == null ? "" : request.requestId())
            .setRequestId(request.correlationId() == null ? "" : request.correlationId())
            .build();

    AskCaseManagerRequest.CaseContext.Builder caseContext =
        AskCaseManagerRequest.CaseContext.newBuilder().setCaseId(request.caseId());
    if (request.endUserId() != null) {
      caseContext.setEndUserId(request.endUserId());
    }

    AskCaseManagerRequest.Builder builder =
        AskCaseManagerRequest.newBuilder()
            .setRequestContext(requestContext)
            .setOperatorId(request.operatorId() == null ? "" : request.operatorId())
            .setPrompt(request.message())
            .setCaseContext(caseContext.build());

    if (request.history() != null) {
      request.history().forEach(turn -> builder.addHistory(toConversationTurn(turn)));
    }
    return builder.build();
  }

  /**
   * The wire contract's {@code ConversationTurn} is a {@code user}/{@code agent} oneof rather than
   * this backend's own {@code role}/{@code content} shape — {@code role} is assumed "user" vs.
   * anything else meaning the agent's own prior turn, matching {@code MlAgentRequest.HistoryTurn}'s
   * existing assumption.
   */
  private static ConversationTurn toConversationTurn(MlAgentRequest.HistoryTurn turn) {
    ConversationTurn.Builder builder = ConversationTurn.newBuilder();
    if ("user".equalsIgnoreCase(turn.role())) {
      builder.setUser(ConversationTurn.UserTurn.newBuilder().setPrompt(turn.content()));
    } else {
      builder.setAgent(ConversationTurn.AgentTurn.newBuilder().setText(turn.content()));
    }
    return builder.build();
  }

  private static CaseSummaryPayload toDomainPayload(CaseManagerAnswerPayload proto) {
    List<CaseSummaryPayload.KeySignal> keySignals =
        proto.getKeySignalsList().stream()
            .map(
                s ->
                    new CaseSummaryPayload.KeySignal(
                        s.getSignal(), List.copyOf(s.getCitationsList())))
            .toList();
    List<CaseSummaryPayload.Citation> citations =
        proto.getCitationsList().stream()
            .map(
                c ->
                    new CaseSummaryPayload.Citation(
                        c.getId(), c.getSource(), List.copyOf(c.getFieldsList())))
            .toList();
    return new CaseSummaryPayload(keySignals, citations);
  }
}
