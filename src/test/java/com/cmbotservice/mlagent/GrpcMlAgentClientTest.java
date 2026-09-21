package com.cmbotservice.mlagent;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
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
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.test.StepVerifier;

/**
 * Exercises {@link GrpcMlAgentClient} against a real (in-process) gRPC server, proving the {@code
 * Flux} bridging, request field mapping, and response validation actually work end to end — not
 * just that the code compiles. In-process transport is the gRPC analog of what MockWebServer was
 * for the old HTTP integration: a real server/channel pair, just without a real OS socket.
 */
class GrpcMlAgentClientTest {

  private Server server;
  private ManagedChannel channel;
  private final AtomicReference<AskCaseManagerRequest> capturedRequest = new AtomicReference<>();

  private GrpcMlAgentClient startClientWith(Consumer<StreamObserver<AnswerEvent>> script)
      throws IOException {
    String serverName = InProcessServerBuilder.generateName();
    ChatAgentGrpc.ChatAgentImplBase service =
        new ChatAgentGrpc.ChatAgentImplBase() {
          @Override
          public void askCaseManager(
              AskCaseManagerRequest request, StreamObserver<AnswerEvent> responseObserver) {
            capturedRequest.set(request);
            script.accept(responseObserver);
          }
        };
    server =
        InProcessServerBuilder.forName(serverName)
            .directExecutor()
            .addService(service)
            .build()
            .start();
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
    return new MlAgentRequest(
        "tenant-1",
        "org-1",
        "case-1",
        List.of(
            new MlAgentRequest.HistoryTurn("user", "hi"),
            new MlAgentRequest.HistoryTurn("assistant", "hello")),
        "msg-1",
        "analyst-1",
        "gadi5",
        "corr-1",
        "req-1",
        "hello");
  }

  private static final CaseManagerAnswerPayload VALID_PAYLOAD =
      CaseManagerAnswerPayload.newBuilder()
          .addKeySignals(
              CaseManagerAnswerPayload.KeySignal.newBuilder()
                  .setSignal("s")
                  .addCitations("c1")
                  .build())
          .addCitations(
              CaseManagerAnswerPayload.Citation.newBuilder()
                  .setId("c1")
                  .setSource("APP_EVENT_LOG")
                  .addFields("risk_score")
                  .build())
          .build();

  @Test
  void
      successfulStream_isParsedIntoDomainEvents_toolCallAndResultAreForwarded_andPingIsConsumedSilently()
          throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("Hello")).build());
              observer.onNext(
                  AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta(" world")).build());
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setToolCall(
                          ToolCall.newBuilder()
                              .setToolCallId("t1")
                              .setName("lookup")
                              .setArgsJson("{\"caseId\":\"case-1\"}"))
                      .build());
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setToolResult(
                          ToolResult.newBuilder()
                              .setToolCallId("t1")
                              .setMs(12)
                              .setRowCount(3)
                              .setStatus(ToolResult.Status.STATUS_OK))
                      .build());
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setPing(com.cmbotservice.mlagent.grpc.v1.Ping.newBuilder())
                      .build());
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setPayload(
                          AnswerPayload.newBuilder().setCaseManagerAnswerPayload(VALID_PAYLOAD))
                      .build());
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setDone(
                          Done.newBuilder()
                              .setStopReason(Done.StopReason.STOP_REASON_COMPLETED)
                              .setLatencyMs(100)
                              .setTokensIn(5)
                              .setTokensOut(10))
                      .build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Token t
                    && t.sequence() == 1
                    && t.delta().equals("Hello"))
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Token t
                    && t.sequence() == 2
                    && t.delta().equals(" world"))
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.ToolCall tc
                    && tc.toolCallId().equals("t1")
                    && tc.name().equals("lookup")
                    && tc.argsJson().equals("{\"caseId\":\"case-1\"}"))
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.ToolResult tr
                    && tr.toolCallId().equals("t1")
                    && tr.status() == MlAgentStreamEvent.ToolResult.Status.OK
                    && tr.ms() == 12
                    && tr.rowCount() == 3)
        // ping is consumed silently: no domain event for it between tool_result and payload.
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Payload p
                    && p.payload().keySignals().get(0).signal().equals("s")
                    && p.payload().citations().get(0).id().equals("c1"))
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.Done d
                    && !d.truncated()
                    && d.latencyMs() == 100
                    && d.tokensIn() == 5
                    && d.tokensOut() == 10)
        .verifyComplete();
  }

  @Test
  void toolResult_withoutRowCount_isMappedToNullRowCount() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setToolResult(
                          ToolResult.newBuilder()
                              .setToolCallId("t1")
                              .setMs(5)
                              .setStatus(ToolResult.Status.STATUS_FAILED))
                      .build());
              observer.onNext(AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.ToolResult tr
                    && tr.status() == MlAgentStreamEvent.ToolResult.Status.FAILED
                    && tr.rowCount() == null)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done)
        .verifyComplete();
  }

  @Test
  void toolResult_withUnspecifiedStatus_isMappedToFailedPerContract() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setToolResult(
                          ToolResult.newBuilder()
                              .setToolCallId("t1")
                              .setStatus(ToolResult.Status.STATUS_UNSPECIFIED))
                      .build());
              observer.onNext(AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(
            e ->
                e instanceof MlAgentStreamEvent.ToolResult tr
                    && tr.status() == MlAgentStreamEvent.ToolResult.Status.FAILED)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done)
        .verifyComplete();
  }

  @Test
  void requestFields_areMappedToTheProtoWireContract() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setDone(
                          Done.newBuilder().setStopReason(Done.StopReason.STOP_REASON_COMPLETED))
                      .build());
              observer.onCompleted();
            });

    client.streamResponse(request()).blockLast(Duration.ofSeconds(5));

    AskCaseManagerRequest sent = capturedRequest.get();
    assertThat(sent).isNotNull();
    assertThat(sent.getPrompt()).isEqualTo("hello");
    assertThat(sent.getOperatorId()).isEqualTo("analyst-1");
    assertThat(sent.getRequestContext().getTenant()).isEqualTo("tenant-1");
    assertThat(sent.getRequestContext().getOrganization()).isEqualTo("org-1");
    assertThat(sent.getRequestContext().getRequestId()).isEqualTo("corr-1");
    assertThat(sent.getRequestContext().getAgentSessionId()).isEqualTo("req-1");
    assertThat(sent.getCaseContext().getCaseId()).isEqualTo("case-1");
    assertThat(sent.getCaseContext().getEndUserId()).isEqualTo("gadi5");
    assertThat(sent.getHistoryList()).hasSize(2);
    assertThat(sent.getHistory(0).getUser().getPrompt()).isEqualTo("hi");
    assertThat(sent.getHistory(1).getAgent().getText()).isEqualTo("hello");
  }

  @Test
  void malformedPayload_keySignalMissingCitation_isMappedToMalformedResponseException()
      throws IOException {
    CaseManagerAnswerPayload badPayload =
        CaseManagerAnswerPayload.newBuilder()
            .addKeySignals(CaseManagerAnswerPayload.KeySignal.newBuilder().setSignal("s").build())
            .build();
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setPayload(
                          AnswerPayload.newBuilder().setCaseManagerAnswerPayload(badPayload))
                      .build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectError(MlAgentMalformedResponseException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void unsetAnswerEvent_isIgnoredRatherThanErroring() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(AnswerEvent.newBuilder().build()); // no oneof case set
              observer.onNext(AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done)
        .verifyComplete();
  }

  @Test
  void connectionFailure_isMappedToMlAgentUnavailableException() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer ->
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
  void statusCode_isMappedToTheCorrespondingExceptionType(
      String statusCodeName, String expectedExceptionClassName) throws Exception {
    Status.Code code = Status.Code.valueOf(statusCodeName);
    Class<?> expectedType = Class.forName(expectedExceptionClassName);
    GrpcMlAgentClient client =
        startClientWith(
            observer ->
                observer.onError(
                    Status.fromCode(code).withDescription("boom").asRuntimeException()));

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
  void statusCode_isMappedToMlAgentRejectedExceptionWithTheRightErrorCode(
      String statusCodeName, String expectedErrorCode) throws IOException {
    Status.Code code = Status.Code.valueOf(statusCodeName);
    ErrorCode expected = ErrorCode.valueOf(expectedErrorCode);
    GrpcMlAgentClient client =
        startClientWith(
            observer ->
                observer.onError(
                    Status.fromCode(code).withDescription("boom").asRuntimeException()));

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectErrorSatisfies(
            err -> {
              assertThat(err).isInstanceOf(MlAgentRejectedException.class);
              assertThat(((MlAgentRejectedException) err).errorCode()).isEqualTo(expected);
            })
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest(name = "in-stream error code {0} -> {1}")
  @CsvSource({
    "ERROR_CODE_MODEL_REFUSED, ML_AGENT_REFUSED",
    "ERROR_CODE_DATA_UNAVAILABLE, ML_AGENT_ERROR",
    "ERROR_CODE_INTERNAL, INTERNAL_ERROR",
    "ERROR_CODE_UNSPECIFIED, INTERNAL_ERROR"
  })
  void nonRetryableInStreamError_isMappedToMlAgentRejectedExceptionWithTheRightErrorCode(
      String protoCode, String expectedErrorCode) throws Exception {
    Error.Code code = Error.Code.valueOf(protoCode);
    ErrorCode expected = ErrorCode.valueOf(expectedErrorCode);
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setError(Error.newBuilder().setCode(code).setRetryable(false))
                      .build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectErrorSatisfies(
            err -> {
              assertThat(err).isInstanceOf(MlAgentRejectedException.class);
              assertThat(((MlAgentRejectedException) err).errorCode()).isEqualTo(expected);
            })
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void retryableInStreamError_isMappedToMlAgentCommunicationException() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(
                  AnswerEvent.newBuilder()
                      .setError(
                          Error.newBuilder()
                              .setCode(Error.Code.ERROR_CODE_DATA_UNAVAILABLE)
                              .setRetryable(true))
                      .build());
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
        .expectError(MlAgentCommunicationException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void agentHistoryTurn_isMappedToTheAgentOneofArm() throws IOException {
    MlAgentRequest requestWithAgentTurn =
        new MlAgentRequest(
            "tenant-1",
            "org-1",
            "case-1",
            List.of(new MlAgentRequest.HistoryTurn("assistant", "hello there")),
            "msg-1",
            "analyst-1",
            null,
            "corr-1",
            "req-1",
            "hi");
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
              observer.onCompleted();
            });

    client.streamResponse(requestWithAgentTurn).blockLast(Duration.ofSeconds(5));

    ConversationTurn turn = capturedRequest.get().getHistory(0);
    assertThat(turn.getTurnCase()).isEqualTo(ConversationTurn.TurnCase.AGENT);
    assertThat(turn.getAgent().getText()).isEqualTo("hello there");
  }
}
