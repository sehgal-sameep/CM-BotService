package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.LogSanitizer;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.mlagent.grpc.v1.AgentRequestContext;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.AskCaseManagerRequest;
import com.cmbotservice.mlagent.grpc.v1.CaseManagerAnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.ChatAgentGrpc;
import com.cmbotservice.mlagent.grpc.v1.ConversationTurn;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

/**
 * Real {@link MlAgentClient} implementation: calls Thoughtful Labs' ML Agent over gRPC's
 * server-streaming {@code ChatAgent.AskCaseManager} RPC (see {@code
 * src/main/proto/chat_agent.proto}) and relays its {@link AnswerEvent}s exactly as received — every
 * event type (<code>chunk</code>, <code>tool_call</code>, <code>tool_result</code>, <code>payload
 * </code>, <code>done</code>, <code>error</code>, <code>ping</code>) and every field, with no
 * translation into a backend-owned model. Never buffers the response — no {@code
 * collectList()}/{@code .block()}, just a straight {@code Flux}.
 *
 * <p>The only event ever withheld is one whose {@code event} oneof is unset — an arm this build's
 * generated code doesn't know yet, or an empty message. Per the contract, "Clients MUST ignore
 * events whose `event` oneof is unset or unrecognised and continue reading the stream", and there
 * is nothing recognisable left in it to forward. Everything else is only <i>observed</i> here
 * (debug/warn logging), never altered.
 *
 * <p>grpc-java's generated async stub is callback-based ({@code StreamObserver}), not {@code
 * Flux}-based — {@link #grpcEventFlux} bridges the two manually via {@code Flux.create} plus
 * grpc-java's own manual flow-control API ({@code disableAutoRequestWithInitial(0)}/{@code
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
  public Flux<AnswerEvent> streamResponse(MlAgentRequest request) {
    AskCaseManagerRequest protoRequest = toProtoRequest(request);
    return Flux.defer(
            () -> {
              log.info(
                  "GRPC_CALL_STARTED messageId={} rpc=ChatAgent/AskCaseManager target={}"
                      + " historyTurns={} promptLength={}",
                  request.messageId(),
                  chatAgentStub.getChannel().authority(),
                  protoRequest.getHistoryCount(),
                  protoRequest.getPrompt().length());
              return grpcEventFlux(protoRequest);
            })
        .filter(GrpcMlAgentClient::isForwardable)
        .doOnComplete(
            () ->
                log.info(
                    "GRPC_CALL_COMPLETED messageId={} target={} status=OK",
                    request.messageId(),
                    chatAgentStub.getChannel().authority()))
        .onErrorMap(io.grpc.StatusRuntimeException.class, ex -> mapStatus(request.messageId(), ex));
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
                  //
                  // Zero initial requests, not disableAutoInboundFlowControl(): that one is
                  // disableAutoRequestWithInitial(1), i.e. gRPC auto-requests one message
                  // on top of every request(n) forwarded from Reactor below. The agent can
                  // then deliver one event more than downstream asked for, which
                  // OverflowStrategy.ERROR rejects with OverflowException whenever demand
                  // is bounded (as it is for the SSE writer) and the agent sends a burst.
                  // With 0, gRPC delivers exactly what Reactor requests.
                  callStream.disableAutoRequestWithInitial(0);
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

  /**
   * Decides only whether an event is forwarded at all — never what it contains. Per-event INFO
   * logging lives in {@code ChatOrchestrationServiceImpl}, shared by every {@link MlAgentClient};
   * this class only warns about contract anomalies it can see on the wire.
   */
  private static boolean isForwardable(AnswerEvent event) {
    if (event.getEventCase() == AnswerEvent.EventCase.EVENT_NOT_SET) {
      log.warn(
          "GRPC_EVENT_IGNORED reason=AnswerEvent has no recognised event set (empty, or an event"
              + " type newer than this build's .proto) — skipped per contract");
      return false;
    }
    if (event.getEventCase() == AnswerEvent.EventCase.PAYLOAD) {
      warnOnPayloadContractViolations(event.getPayload());
    }
    return true;
  }

  private static void warnOnPayloadContractViolations(AnswerPayload payload) {
    if (payload.getPayloadCase() != AnswerPayload.PayloadCase.CASE_MANAGER_ANSWER_PAYLOAD) {
      log.warn(
          "GRPC_PAYLOAD_CONTRACT_VIOLATION reason=no recognised payload arm set — forwarding"
              + " as-is");
      return;
    }
    for (CaseManagerAnswerPayload.KeySignal signal :
        payload.getCaseManagerAnswerPayload().getKeySignalsList()) {
      if (signal.getCitationsCount() == 0) {
        log.warn(
            "GRPC_PAYLOAD_CONTRACT_VIOLATION reason=key signal without a citation — forwarding"
                + " as-is");
      }
    }
  }

  /**
   * gRPC-level rejections/failures — the direct analog of the old HTTP integration's pre-stream
   * 401/403/404/422/429/503 status mapping, just keyed off {@link Status.Code} instead of an HTTP
   * status.
   */
  private Throwable mapStatus(String messageId, io.grpc.StatusRuntimeException ex) {
    Status.Code code = ex.getStatus().getCode();
    // The gRPC status is the ML Agent's (or the channel's) own verdict — log it verbatim,
    // with its description and root cause, before it is mapped to our exception types.
    log.error(
        "GRPC_CALL_FAILED messageId={} target={} status={} description='{}' cause=[{}]",
        messageId,
        chatAgentStub.getChannel().authority(),
        code,
        ex.getStatus().getDescription(),
        ex.getCause() == null ? "none" : LogSanitizer.causeChain(ex.getCause()));
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
        AskCaseManagerRequest.CaseContext.newBuilder()
            .setCaseId(request.caseId() == null ? "" : request.caseId());
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
}
