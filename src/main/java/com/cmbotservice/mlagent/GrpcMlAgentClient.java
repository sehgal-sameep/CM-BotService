package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.mlagent.grpc.v1.ChatAgentGrpc;
import com.cmbotservice.mlagent.grpc.v1.ChatEvent;
import com.cmbotservice.mlagent.grpc.v1.ChatRequest;
import com.cmbotservice.mlagent.grpc.v1.Done;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.HistoryTurn;
import com.cmbotservice.mlagent.grpc.v1.Payload;
import com.cmbotservice.mlagent.grpc.v1.Token;
import com.cmbotservice.mlagent.grpc.v1.ToolCall;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import io.grpc.Status;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Real {@link MlAgentClient} implementation: calls Thoughtful Labs' ML Agent over
 * gRPC's server-streaming {@code ChatAgent.Chat} RPC (see
 * {@code src/main/proto/chat_agent.proto}), translating its six event types
 * (<code>token</code>, <code>tool_call</code>, <code>tool_result</code>,
 * <code>payload</code>, <code>done</code>, <code>error</code>) into
 * {@link MlAgentStreamEvent}. Never buffers the response — no
 * {@code collectList()}/{@code .block()}, just a straight {@code Flux}.
 * <p>
 * {@code tool_call}/{@code tool_result} are consumed and logged only, per the real
 * contract's own note that they're "rendered in the sandbox trace, logged in
 * product" — this backend is the product, not the sandbox, so they never become a
 * domain event.
 * <p>
 * grpc-java's generated async stub is callback-based ({@code StreamObserver}), not
 * {@code Flux}-based — {@link #grpcEventFlux} bridges the two manually via
 * {@code Flux.create} plus grpc-java's own manual flow-control API
 * ({@code disableAutoInboundFlowControl}/{@code request(n)}), giving real backpressure
 * without pulling in a third-party reactive-grpc codegen plugin.
 * <p>
 * No client-side gRPC deadline is set here: {@code ChatOrchestrationService}'s
 * existing first-response/idle {@code .timeout()} operator is the one timeout
 * authority, client-agnostic — a second, competing deadline at this layer would just
 * be a redundant knob.
 * <p>
 * Selected via {@code ml-agent.mode: grpc}; {@link MockMlAgentClient} steps aside
 * automatically ({@code @ConditionalOnProperty} on both, never a runtime check here).
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
        ChatRequest protoRequest = toProtoRequest(request);

        Flux<MlAgentStreamEvent> events = grpcEventFlux(protoRequest)
                .<MlAgentStreamEvent>handle((event, sink) -> emit(event, lastSequence, sink))
                .onErrorMap(io.grpc.StatusRuntimeException.class, this::mapStatus);

        return Flux.concat(Mono.just(new MlAgentStreamEvent.Started()), events);
    }

    /**
     * Bridges grpc-java's callback-based async stub into a cold, backpressure-respecting
     * {@code Flux}: nothing is sent to the wire until subscribed ({@code stub.chat(...)}
     * runs inside the {@code Flux.create} lambda), downstream demand is translated into
     * {@code ClientCallStreamObserver#request(int)} calls, and cancellation (a client
     * disconnect propagating down from {@code ChatOrchestrationService}) calls
     * {@code ClientCallStreamObserver#cancel(...)} to actually stop the server-side call.
     */
    private Flux<ChatEvent> grpcEventFlux(ChatRequest protoRequest) {
        return Flux.create(sink -> {
            AtomicReference<ClientCallStreamObserver<ChatRequest>> callStreamRef = new AtomicReference<>();
            ClientResponseObserver<ChatRequest, ChatEvent> observer = new ClientResponseObserver<>() {
                @Override
                public void beforeStart(ClientCallStreamObserver<ChatRequest> callStream) {
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
                public void onNext(ChatEvent event) {
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
            chatAgentStub.chat(protoRequest, observer);
            // chatAgentStub.chat(...) has now returned, meaning start() has already run (grpc-java
            // calls it synchronously as part of this method) — request()/cancel() are safe from here on.
            ClientCallStreamObserver<ChatRequest> callStream = callStreamRef.get();
            sink.onRequest(n -> callStream.request(n >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) n));
            sink.onCancel(() -> callStream.cancel("Downstream cancelled", null));
        }, FluxSink.OverflowStrategy.ERROR);
    }

    private void emit(ChatEvent event, AtomicInteger lastSequence, SynchronousSink<MlAgentStreamEvent> sink) {
        switch (event.getEventCase()) {
            case TOKEN -> sink.next(toToken(event.getToken(), lastSequence));
            case TOOL_CALL -> logToolCall(event.getToolCall());
            case TOOL_RESULT -> logToolResult(event.getToolResult());
            case PAYLOAD -> sink.next(toPayload(event.getPayload()));
            case DONE -> sink.next(toDone(event.getDone()));
            case ERROR -> sink.error(toErrorException(event.getError()));
            case EVENT_NOT_SET ->
                    sink.error(new MlAgentMalformedResponseException("ML Agent ChatEvent had no event set"));
        }
    }

    private static MlAgentStreamEvent.Token toToken(Token token, AtomicInteger lastSequence) {
        return new MlAgentStreamEvent.Token(token.getDelta(), lastSequence.incrementAndGet());
    }

    private static MlAgentStreamEvent.Payload toPayload(Payload proto) {
        CaseSummaryPayload payload = toDomainPayload(proto);
        CaseSummaryPayloadValidator.validate(payload);
        return new MlAgentStreamEvent.Payload(payload);
    }

    private static MlAgentStreamEvent.Done toDone(Done done) {
        return new MlAgentStreamEvent.Done(
                done.getConversationId(), done.getContinuation(),
                done.getLatencyMs(), done.getTokensIn(), done.getTokensOut());
    }

    private static Throwable toErrorException(Error error) {
        String message = error.getMessage().isEmpty() ? "The ML Agent reported an error" : error.getMessage();
        return mapErrorCode(error.getCode(), message);
    }

    /**
     * One unified error-code space the real contract uses both as a gRPC-level status
     * (see {@link #mapStatus}) and, for codes only discoverable mid-stream, as this
     * terminal {@code error} event's {@code code} field — identical mapping to what
     * the original HTTP/SSE integration used.
     */
    private static Throwable mapErrorCode(String code, String message) {
        return switch (code) {
            case "401", "403" -> new MlAgentRejectedException(ErrorCode.INTERNAL_ERROR, message);
            case "404" -> new MlAgentRejectedException(ErrorCode.NOT_FOUND, message);
            case "422" -> new MlAgentRejectedException(ErrorCode.VALIDATION_ERROR, message);
            case "429", "503" -> new MlAgentCommunicationException(message);
            case "4221", "4222" -> new MlAgentContinuationExpiredException(message);
            default -> new MlAgentCommunicationException(message);
        };
    }

    /**
     * gRPC-level rejections/failures — the direct analog of the old HTTP integration's
     * pre-stream 401/403/404/422/429/503 status mapping, just keyed off
     * {@link Status.Code} instead of an HTTP status.
     */
    private Throwable mapStatus(io.grpc.StatusRuntimeException ex) {
        Status.Code code = ex.getStatus().getCode();
        return switch (code) {
            case UNAVAILABLE, UNKNOWN -> new MlAgentUnavailableException("Could not reach the ML Agent", ex);
            case RESOURCE_EXHAUSTED -> new MlAgentCommunicationException(
                    "ML Agent is temporarily unavailable (status " + code + ")", ex);
            case UNAUTHENTICATED, PERMISSION_DENIED -> new MlAgentRejectedException(ErrorCode.INTERNAL_ERROR,
                    "The ML Agent rejected our credentials or tenant access");
            case NOT_FOUND -> new MlAgentRejectedException(ErrorCode.NOT_FOUND, "Case not found in that tenant");
            case INVALID_ARGUMENT -> new MlAgentRejectedException(ErrorCode.VALIDATION_ERROR,
                    "Malformed request or missing required context");
            default -> new MlAgentCommunicationException("ML Agent returned an unexpected status " + code, ex);
        };
    }

    private void logToolCall(ToolCall toolCall) {
        log.debug("ML Agent tool_call id={} name={}", toolCall.getId(), toolCall.getName());
    }

    private void logToolResult(ToolResult toolResult) {
        log.debug("ML Agent tool_result id={} ms={} rowCount={} ok={}",
                toolResult.getId(), toolResult.getMs(), toolResult.getRowCount(), toolResult.getOk());
    }

    private static ChatRequest toProtoRequest(MlAgentRequest request) {
        ChatRequest.Context.Builder context = ChatRequest.Context.newBuilder().setCaseId(request.caseId());
        if (request.endUserId() != null) {
            context.setEndUserId(request.endUserId());
        }

        ChatRequest.Builder builder = ChatRequest.newBuilder()
                .setSurface(request.surface())
                .setMessage(request.message())
                .setTenantId(request.tenantId())
                .setContext(context.build())
                .setOptions(ChatRequest.Options.newBuilder()
                        .setIncludeResolutions(request.includeResolutions())
                        .build());

        if (request.continuation() != null) {
            builder.setContinuation(request.continuation());
        }
        if (request.conversationId() != null) {
            builder.setConversationId(request.conversationId());
        }
        if (request.history() != null) {
            request.history().forEach(turn -> builder.addHistory(
                    HistoryTurn.newBuilder().setRole(turn.role()).setContent(turn.content()).build()));
        }
        return builder.build();
    }

    private static CaseSummaryPayload toDomainPayload(Payload proto) {
        CaseSummaryPayload.Summary summary = proto.hasSummary() ? toDomainSummary(proto.getSummary()) : null;
        CaseSummaryPayload.SuggestedResolution resolution = proto.hasSuggestedResolution()
                ? toDomainResolution(proto.getSuggestedResolution()) : null;
        List<CaseSummaryPayload.Citation> citations = proto.getCitationsList().stream()
                .map(c -> new CaseSummaryPayload.Citation(c.getId(), c.getSource(), List.copyOf(c.getFieldsList())))
                .toList();
        return new CaseSummaryPayload(proto.getAnswer(), summary, resolution, citations);
    }

    private static CaseSummaryPayload.Summary toDomainSummary(Payload.Summary proto) {
        List<CaseSummaryPayload.KeySignal> keySignals = proto.getKeySignalsList().stream()
                .map(s -> new CaseSummaryPayload.KeySignal(s.getSignal(), s.getSeverity(), List.copyOf(s.getCitationsList())))
                .toList();
        List<CaseSummaryPayload.Entity> entities = proto.getEntitiesList().stream()
                .map(e -> new CaseSummaryPayload.Entity(e.getType(), e.getValue(), List.copyOf(e.getEventsList())))
                .toList();
        List<CaseSummaryPayload.TimelineEvent> timeline = proto.getTimelineList().stream()
                .map(t -> new CaseSummaryPayload.TimelineEvent(t.getAt(), t.getEventId(), t.getWhat()))
                .toList();
        return new CaseSummaryPayload.Summary(proto.getNarrative(), keySignals, entities, timeline);
    }

    private static CaseSummaryPayload.SuggestedResolution toDomainResolution(Payload.SuggestedResolution proto) {
        return new CaseSummaryPayload.SuggestedResolution(
                proto.getMark(), proto.getLabel(), proto.getConfidence(), proto.getRationale());
    }
}
