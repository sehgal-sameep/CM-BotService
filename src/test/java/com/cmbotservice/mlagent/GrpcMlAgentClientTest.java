package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
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
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.groups.Tuple.tuple;

/**
 * Exercises {@link GrpcMlAgentClient} against a real (in-process) gRPC server, proving
 * the {@code Flux} bridging, request field mapping, and response validation actually
 * work end to end — not just that the code compiles. In-process transport is the gRPC
 * analog of what MockWebServer was for the old HTTP integration: a real server/channel
 * pair, just without a real OS socket.
 */
class GrpcMlAgentClientTest {

    private Server server;
    private ManagedChannel channel;
    private final AtomicReference<ChatRequest> capturedRequest = new AtomicReference<>();

    private GrpcMlAgentClient startClientWith(Consumer<StreamObserver<ChatEvent>> script) throws IOException {
        String serverName = InProcessServerBuilder.generateName();
        ChatAgentGrpc.ChatAgentImplBase service = new ChatAgentGrpc.ChatAgentImplBase() {
            @Override
            public void chat(ChatRequest request, StreamObserver<ChatEvent> responseObserver) {
                capturedRequest.set(request);
                script.accept(responseObserver);
            }
        };
        server = InProcessServerBuilder.forName(serverName).directExecutor().addService(service).build().start();
        channel = InProcessChannelBuilder.forName(serverName).directExecutor().build();
        return new GrpcMlAgentClient(ChatAgentGrpc.newStub(channel));
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.shutdownNow();
        }
        if (server != null) {
            server.shutdownNow();
        }
    }

    private static MlAgentRequest request() {
        return new MlAgentRequest("tenant-1", "case-1", "cont-0", "conv-0",
                List.of(new MlAgentRequest.HistoryTurn("user", "hi"), new MlAgentRequest.HistoryTurn("assistant", "hello")),
                "msg-1", "analyst-1", "gadi5", "corr-1", "req-1", MlAgentRequest.SURFACE_CASE_MANAGER, true, "hello");
    }

    private static final Payload VALID_PAYLOAD = Payload.newBuilder()
            .setAnswer("Hello world")
            .setSummary(Payload.Summary.newBuilder()
                    .setNarrative("n")
                    .addKeySignals(Payload.Summary.KeySignal.newBuilder()
                            .setSignal("s").setSeverity("high").addCitations("c1").build())
                    .build())
            .setSuggestedResolution(Payload.SuggestedResolution.newBuilder()
                    .setMark("SUSPECTED_FRAUD").setLabel("Suspected Fraud").setConfidence("medium").setRationale("r").build())
            .addCitations(Payload.Citation.newBuilder().setId("c1").setSource("APP_EVENT_LOG").addFields("risk_score").build())
            .build();

    @Test
    void successfulStream_isParsedIntoDomainEvents_andToolEventsAreConsumedSilently() throws IOException {
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setToken(Token.newBuilder().setDelta("Hello")).build());
            observer.onNext(ChatEvent.newBuilder().setToken(Token.newBuilder().setDelta(" world")).build());
            observer.onNext(ChatEvent.newBuilder()
                    .setToolCall(ToolCall.newBuilder().setId("t1").setName("lookup")).build());
            observer.onNext(ChatEvent.newBuilder()
                    .setToolResult(ToolResult.newBuilder().setId("t1").setMs(12).setRowCount(3).setOk(true)).build());
            observer.onNext(ChatEvent.newBuilder().setPayload(VALID_PAYLOAD).build());
            observer.onNext(ChatEvent.newBuilder().setDone(Done.newBuilder()
                    .setConversationId("conv-1").setContinuation("cont-1")
                    .setLatencyMs(100).setTokensIn(5).setTokensOut(10)).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token t
                        && t.sequence() == 1 && t.delta().equals("Hello"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token t
                        && t.sequence() == 2 && t.delta().equals(" world"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Payload p
                        && p.payload().answer().equals("Hello world")
                        && p.payload().suggestedResolution().mark().equals("SUSPECTED_FRAUD"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done d
                        && d.conversationId().equals("conv-1") && d.continuation().equals("cont-1"))
                .verifyComplete();
    }

    @Test
    void requestFields_areMappedToTheProtoWireContract() throws IOException {
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setDone(Done.newBuilder()
                    .setConversationId("conv-1").setContinuation("cont-1")).build());
            observer.onCompleted();
        });

        client.streamResponse(request()).blockLast(Duration.ofSeconds(5));

        ChatRequest sent = capturedRequest.get();
        assertThat(sent).isNotNull();
        assertThat(sent.getContinuation()).isEqualTo("cont-0");
        assertThat(sent.getConversationId()).isEqualTo("conv-0");
        assertThat(sent.getSurface()).isEqualTo("case_manager");
        assertThat(sent.getMessage()).isEqualTo("hello");
        assertThat(sent.getTenantId()).isEqualTo("tenant-1");
        assertThat(sent.getContext().getCaseId()).isEqualTo("case-1");
        assertThat(sent.getContext().getEndUserId()).isEqualTo("gadi5");
        assertThat(sent.getOptions().getIncludeResolutions()).isTrue();
        assertThat(sent.getHistoryList())
                .extracting(HistoryTurn::getRole, HistoryTurn::getContent)
                .containsExactly(tuple("user", "hi"), tuple("assistant", "hello"));
    }

    @Test
    void malformedPayload_keySignalMissingCitation_isMappedToMalformedResponseException() throws IOException {
        Payload badPayload = Payload.newBuilder()
                .setAnswer("a")
                .setSummary(Payload.Summary.newBuilder()
                        .addKeySignals(Payload.Summary.KeySignal.newBuilder()
                                .setSignal("s").setSeverity("high").build())
                        .build())
                .build();
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setPayload(badPayload).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void malformedPayload_unknownResolutionMark_isMappedToMalformedResponseException() throws IOException {
        Payload badPayload = Payload.newBuilder()
                .setAnswer("a")
                .setSuggestedResolution(Payload.SuggestedResolution.newBuilder().setMark("Z").build())
                .build();
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setPayload(badPayload).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    /**
     * The single-character codes (F/S/G/A/U/Y/B/T) are a different system's
     * (APP_EVENT_UPDATE.CUSTOM_MARK) internal representation and, per the contract,
     * never appear on this interface — so one showing up here is exactly as invalid as
     * any other unrecognized string.
     */
    @Test
    void malformedPayload_legacySingleCharacterMark_isMappedToMalformedResponseException() throws IOException {
        Payload badPayload = Payload.newBuilder()
                .setAnswer("a")
                .setSuggestedResolution(Payload.SuggestedResolution.newBuilder().setMark("S").build())
                .build();
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setPayload(badPayload).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    /**
     * {@code ANY} is documented as "filter-only" — the contract states the agent must
     * never emit it, so its presence in a real response is itself a contract
     * violation this backend should surface loudly, not silently accept.
     */
    @Test
    void malformedPayload_filterOnlyAnyMark_isMappedToMalformedResponseException() throws IOException {
        Payload badPayload = Payload.newBuilder()
                .setAnswer("a")
                .setSuggestedResolution(Payload.SuggestedResolution.newBuilder().setMark("ANY").build())
                .build();
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().setPayload(badPayload).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void eventWithNoOneofCaseSet_isMappedToMalformedResponseException() throws IOException {
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder().build()); // no oneof case set
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void connectionFailure_isMappedToMlAgentUnavailableException() throws IOException {
        GrpcMlAgentClient client = startClientWith(observer ->
                observer.onError(Status.UNAVAILABLE.withDescription("down").asRuntimeException()));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentUnavailableException.class)
                .verify(Duration.ofSeconds(5));
    }

    @ParameterizedTest(name = "gRPC status {0} -> {1}")
    @CsvSource({
            "UNAVAILABLE, com.cmbotservice.common.MlAgentUnavailableException",
            "UNKNOWN, com.cmbotservice.common.MlAgentUnavailableException",
            "RESOURCE_EXHAUSTED, com.cmbotservice.common.MlAgentCommunicationException",
            "INTERNAL, com.cmbotservice.common.MlAgentCommunicationException"
    })
    void statusCode_isMappedToTheCorrespondingExceptionType(String statusCodeName, String expectedExceptionClassName)
            throws Exception {
        Status.Code code = Status.Code.valueOf(statusCodeName);
        Class<?> expectedType = Class.forName(expectedExceptionClassName);
        GrpcMlAgentClient client = startClientWith(observer ->
                observer.onError(Status.fromCode(code).withDescription("boom").asRuntimeException()));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectErrorMatches(expectedType::isInstance)
                .verify(Duration.ofSeconds(5));
    }

    @ParameterizedTest(name = "gRPC status {0} -> MlAgentRejectedException({1})")
    @CsvSource({
            "UNAUTHENTICATED, INTERNAL_ERROR",
            "PERMISSION_DENIED, INTERNAL_ERROR",
            "NOT_FOUND, NOT_FOUND",
            "INVALID_ARGUMENT, VALIDATION_ERROR"
    })
    void statusCode_isMappedToMlAgentRejectedExceptionWithTheRightErrorCode(String statusCodeName, String expectedErrorCode)
            throws IOException {
        Status.Code code = Status.Code.valueOf(statusCodeName);
        ErrorCode expected = ErrorCode.valueOf(expectedErrorCode);
        GrpcMlAgentClient client = startClientWith(observer ->
                observer.onError(Status.fromCode(code).withDescription("boom").asRuntimeException()));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectErrorSatisfies(err -> {
                    assertThat(err).isInstanceOf(MlAgentRejectedException.class);
                    assertThat(((MlAgentRejectedException) err).errorCode()).isEqualTo(expected);
                })
                .verify(Duration.ofSeconds(5));
    }

    @ParameterizedTest(name = "in-stream error code {0} -> {1}")
    @CsvSource({
            "401, com.cmbotservice.common.MlAgentRejectedException",
            "403, com.cmbotservice.common.MlAgentRejectedException",
            "404, com.cmbotservice.common.MlAgentRejectedException",
            "422, com.cmbotservice.common.MlAgentRejectedException",
            "429, com.cmbotservice.common.MlAgentCommunicationException",
            "503, com.cmbotservice.common.MlAgentCommunicationException",
            "4221, com.cmbotservice.common.MlAgentContinuationExpiredException",
            "4222, com.cmbotservice.common.MlAgentContinuationExpiredException"
    })
    void inStreamErrorEvent_codeIsMappedToTheCorrespondingExceptionType(String code, String expectedExceptionClassName)
            throws Exception {
        Class<?> expectedType = Class.forName(expectedExceptionClassName);
        GrpcMlAgentClient client = startClientWith(observer -> {
            observer.onNext(ChatEvent.newBuilder()
                    .setError(Error.newBuilder().setCode(code).setMessage("simulated").build()).build());
            observer.onCompleted();
        });

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectErrorMatches(expectedType::isInstance)
                .verify(Duration.ofSeconds(5));
    }
}
