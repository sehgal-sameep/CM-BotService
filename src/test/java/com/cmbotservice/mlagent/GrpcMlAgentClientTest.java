package com.cmbotservice.mlagent;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.context.RequestContext;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.AskCaseManagerRequest;
import com.cmbotservice.mlagent.grpc.v1.CaseManagerAnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.ChatAgentGrpc;
import com.cmbotservice.mlagent.grpc.v1.Chunk;
import com.cmbotservice.mlagent.grpc.v1.ConversationTurn;
import com.cmbotservice.mlagent.grpc.v1.Done;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.Ping;
import com.cmbotservice.mlagent.grpc.v1.ToolCall;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import com.cmbotservice.security.SessionContext;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
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
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.LoggerFactory;
import reactor.test.StepVerifier;

/**
 * Exercises {@link GrpcMlAgentClient} against a real (in-process) gRPC server, proving the {@code
 * Flux} bridging, request field mapping, and verbatim (untranslated) event forwarding actually work
 * end to end — not just that the code compiles. In-process transport is the gRPC analog of what
 * MockWebServer was for the old HTTP integration: a real server/channel pair, just without a real
 * OS socket.
 */
class GrpcMlAgentClientTest {

  private Server server;
  private ManagedChannel channel;
  private final AtomicReference<AskCaseManagerRequest> capturedRequest = new AtomicReference<>();
  private final AtomicReference<Metadata> capturedHeaders = new AtomicReference<>();

  /** Records the metadata of every incoming call, the way a real auth interceptor would see it. */
  private final ServerInterceptor headerCapture =
      new ServerInterceptor() {
        @Override
        public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
            ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
          capturedHeaders.set(headers);
          return next.startCall(call, headers);
        }
      };

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
            .addService(ServerInterceptors.intercept(service, headerCapture))
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
    return request(null);
  }

  private static MlAgentRequest request(String accessToken) {
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
        "hello",
        accessToken);
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
  void everyEventType_isForwardedAsTheIdenticalProtoMessage_inOrder_includingPing()
      throws IOException {
    List<AnswerEvent> sent =
        List.of(
            AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("Hello")).build(),
            AnswerEvent.newBuilder()
                .setToolCall(
                    ToolCall.newBuilder()
                        .setToolCallId("t1")
                        .setName("lookup")
                        .setArgsJson("{\"caseId\":\"case-1\"}"))
                .build(),
            AnswerEvent.newBuilder()
                .setToolResult(
                    ToolResult.newBuilder()
                        .setToolCallId("t1")
                        .setMs(12)
                        .setRowCount(3)
                        .setStatus(ToolResult.Status.STATUS_OK))
                .build(),
            AnswerEvent.newBuilder().setPing(Ping.newBuilder()).build(),
            AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta(" world")).build(),
            AnswerEvent.newBuilder()
                .setPayload(AnswerPayload.newBuilder().setCaseManagerAnswerPayload(VALID_PAYLOAD))
                .build(),
            AnswerEvent.newBuilder()
                .setDone(
                    Done.newBuilder()
                        .setStopReason(Done.StopReason.STOP_REASON_TRUNCATED)
                        .setLatencyMs(100)
                        .setTokensIn(5)
                        .setTokensOut(10))
                .build());
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              sent.forEach(observer::onNext);
              observer.onCompleted();
            });

    List<AnswerEvent> received =
        client.streamResponse(request()).collectList().block(Duration.ofSeconds(5));

    // Protobuf equals() is deep field-by-field equality: nothing renamed, dropped,
    // re-typed, reordered, or synthesized (no leading "started" marker of our own).
    assertThat(received).containsExactlyElementsOf(sent);
  }

  @Test
  void boundedDownstreamDemand_isHonouredExactly_evenWhenTheAgentSendsABurst() throws IOException {
    // Regression: disableAutoInboundFlowControl() is disableAutoRequestWithInitial(1) —
    // gRPC auto-requests one message on top of the demand we forward from Reactor, so a
    // burst from the agent arrived with no outstanding demand and Flux.create's
    // OverflowStrategy.ERROR failed the stream with OverflowException. Unbounded-demand
    // tests never see it; the real SSE writer requests in small batches.
    List<AnswerEvent> burst =
        List.of(
            AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("a")).build(),
            AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("b")).build(),
            AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("c")).build(),
            AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
    // Sent from another thread, after the call has started — like a real agent over the
    // network. (Sending synchronously inside the handler delivers the first message before
    // our demand is wired, which happens to mask the extra auto-request.)
    GrpcMlAgentClient client =
        startClientWith(
            observer ->
                new Thread(
                        () -> {
                          try {
                            Thread.sleep(50);
                          } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                          }
                          burst.forEach(observer::onNext);
                          observer.onCompleted();
                        })
                    .start());

    StepVerifier.create(client.streamResponse(request()), 1)
        .expectNext(burst.get(0))
        .expectNoEvent(Duration.ofMillis(100)) // nothing more delivered without demand
        .thenRequest(1)
        .expectNext(burst.get(1))
        .thenRequest(2)
        .expectNext(burst.get(2), burst.get(3))
        .verifyComplete();
  }

  @Test
  void toolResult_withUnspecifiedStatusAndNoRowCount_isForwardedUnchanged() throws IOException {
    AnswerEvent toolResult =
        AnswerEvent.newBuilder()
            .setToolResult(
                ToolResult.newBuilder()
                    .setToolCallId("t1")
                    .setStatus(ToolResult.Status.STATUS_UNSPECIFIED))
            .build();
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(toolResult);
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request()))
        .assertNext(
            e -> {
              assertThat(e).isEqualTo(toolResult);
              // Not folded into STATUS_FAILED, and no row_count invented.
              assertThat(e.getToolResult().getStatus())
                  .isEqualTo(ToolResult.Status.STATUS_UNSPECIFIED);
              assertThat(e.getToolResult().hasRowCount()).isFalse();
            })
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
  void payload_withKeySignalMissingCitation_isStillForwardedAsIs_notRejected() throws IOException {
    AnswerEvent payloadEvent =
        AnswerEvent.newBuilder()
            .setPayload(
                AnswerPayload.newBuilder()
                    .setCaseManagerAnswerPayload(
                        CaseManagerAnswerPayload.newBuilder()
                            .addKeySignals(
                                CaseManagerAnswerPayload.KeySignal.newBuilder().setSignal("s"))))
            .build();
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(payloadEvent);
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request())).expectNext(payloadEvent).verifyComplete();
  }

  @Test
  void unsetAnswerEvent_isIgnoredRatherThanErroring() throws IOException {
    AnswerEvent done = AnswerEvent.newBuilder().setDone(Done.newBuilder()).build();
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(AnswerEvent.newBuilder().build()); // no oneof case set
              observer.onNext(done);
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request())).expectNext(done).verifyComplete();
  }

  @Test
  void connectionFailure_isMappedToMlAgentUnavailableException() throws IOException {
    GrpcMlAgentClient client =
        startClientWith(
            observer ->
                observer.onError(Status.UNAVAILABLE.withDescription("down").asRuntimeException()));

    StepVerifier.create(client.streamResponse(request()))
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
        .expectErrorSatisfies(
            err -> {
              assertThat(err).isInstanceOf(MlAgentRejectedException.class);
              assertThat(((MlAgentRejectedException) err).errorCode()).isEqualTo(expected);
            })
        .verify(Duration.ofSeconds(5));
  }

  @ParameterizedTest(name = "in-stream error code {0}, retryable={1} is forwarded as-is")
  @CsvSource({
    "ERROR_CODE_MODEL_REFUSED, false",
    "ERROR_CODE_DATA_UNAVAILABLE, true",
    "ERROR_CODE_INTERNAL, false",
    "ERROR_CODE_UNSPECIFIED, false"
  })
  void inStreamErrorEvent_isForwardedAsAnEvent_neverConvertedIntoAnException(
      String protoCode, boolean retryable) throws Exception {
    AnswerEvent errorEvent =
        AnswerEvent.newBuilder()
            .setError(
                Error.newBuilder().setCode(Error.Code.valueOf(protoCode)).setRetryable(retryable))
            .build();
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(errorEvent);
              observer.onCompleted();
            });

    StepVerifier.create(client.streamResponse(request())).expectNext(errorEvent).verifyComplete();
  }

  @Test
  void absentOrganizationOperatorIdAndEndUserId_areNotSetOnTheProtoRequest() throws IOException {
    MlAgentRequest withoutIds =
        new MlAgentRequest(
            "tenant-1",
            null,
            "case-1",
            List.of(),
            "msg-1",
            null,
            null,
            "corr-1",
            "req-1",
            "hi",
            null);
    GrpcMlAgentClient client =
        startClientWith(
            observer -> {
              observer.onNext(AnswerEvent.newBuilder().setDone(Done.newBuilder()).build());
              observer.onCompleted();
            });

    client.streamResponse(withoutIds).blockLast(Duration.ofSeconds(5));

    AskCaseManagerRequest sent = capturedRequest.get();
    assertThat(sent.getRequestContext().getOrganization()).isEmpty();
    assertThat(sent.getOperatorId()).isEmpty();
    assertThat(sent.getCaseContext().getEndUserId()).isEmpty();
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
            "hi",
            null);
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

  // --- authorization metadata ---

  private static final String TOKEN = "eyJhbGciOiJSUzI1NiJ9.test-payload.test-signature";
  private static final Metadata.Key<String> AUTHORIZATION =
      Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

  private static final AnswerEvent CHUNK =
      AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("Hello")).build();
  private static final AnswerEvent DONE =
      AnswerEvent.newBuilder().setDone(Done.newBuilder()).build();

  private GrpcMlAgentClient startStreamingClient() throws IOException {
    return startClientWith(
        observer -> {
          observer.onNext(CHUNK);
          observer.onNext(DONE);
          observer.onCompleted();
        });
  }

  @Test
  void accessToken_isSentAsBearerAuthorizationMetadata_andStreamIsUnchanged() throws IOException {
    GrpcMlAgentClient client = startStreamingClient();

    StepVerifier.create(client.streamResponse(request(TOKEN)))
        .expectNext(CHUNK, DONE)
        .verifyComplete();

    assertThat(capturedHeaders.get().getAll(AUTHORIZATION)).containsExactly("Bearer " + TOKEN);
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "   "})
  void missingOrBlankAccessToken_sendsNoAuthorizationMetadata(String accessToken)
      throws IOException {
    GrpcMlAgentClient client = startStreamingClient();

    StepVerifier.create(client.streamResponse(request(accessToken)))
        .expectNext(CHUNK, DONE)
        .verifyComplete();

    assertThat(capturedHeaders.get().containsKey(AUTHORIZATION)).isFalse();
  }

  @Test
  void protoRequest_isIdenticalWithAndWithoutAccessToken() throws IOException {
    GrpcMlAgentClient client = startStreamingClient();

    client.streamResponse(request(null)).blockLast(Duration.ofSeconds(5));
    AskCaseManagerRequest withoutToken = capturedRequest.get();
    client.streamResponse(request(TOKEN)).blockLast(Duration.ofSeconds(5));
    AskCaseManagerRequest withToken = capturedRequest.get();

    assertThat(withToken).isEqualTo(withoutToken);
    assertThat(withToken.toString()).doesNotContain(TOKEN);
  }

  @Test
  void accessToken_isScopedToItsOwnCall_neverReusedByTheSharedStub() throws IOException {
    GrpcMlAgentClient client = startStreamingClient();

    client.streamResponse(request("token-user-a")).blockLast(Duration.ofSeconds(5));
    assertThat(capturedHeaders.get().getAll(AUTHORIZATION)).containsExactly("Bearer token-user-a");

    client.streamResponse(request(null)).blockLast(Duration.ofSeconds(5));
    assertThat(capturedHeaders.get().containsKey(AUTHORIZATION)).isFalse();

    client.streamResponse(request("token-user-b")).blockLast(Duration.ofSeconds(5));
    assertThat(capturedHeaders.get().getAll(AUTHORIZATION)).containsExactly("Bearer token-user-b");
  }

  @Test
  void accessToken_neverAppearsInLogs_evenWhenTheCallFails() throws IOException {
    Logger root = (Logger) LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
    ListAppender<ILoggingEvent> appender = new ListAppender<>();
    appender.start();
    root.addAppender(appender);
    try {
      GrpcMlAgentClient ok = startStreamingClient();
      ok.streamResponse(request(TOKEN)).blockLast(Duration.ofSeconds(5));
      tearDown();

      GrpcMlAgentClient failing =
          startClientWith(
              observer -> observer.onError(Status.UNAUTHENTICATED.asRuntimeException()));
      StepVerifier.create(failing.streamResponse(request(TOKEN)))
          .expectError(MlAgentRejectedException.class)
          .verify(Duration.ofSeconds(5));
    } finally {
      root.detachAppender(appender);
    }

    assertThat(appender.list)
        .extracting(ILoggingEvent::getFormattedMessage)
        .anyMatch(m -> m.contains("accessTokenPresent=true"))
        .noneMatch(m -> m.contains(TOKEN));
    assertThat(appender.list)
        .filteredOn(e -> e.getThrowableProxy() != null)
        .extracting(e -> String.valueOf(e.getThrowableProxy().getMessage()))
        .noneMatch(m -> m.contains(TOKEN));
  }

  @Test
  void toString_ofEveryTokenCarrier_redactsTheToken() {
    assertThat(request(TOKEN).toString()).doesNotContain(TOKEN).contains("accessTokenPresent=true");
    assertThat(new BearerTokenCallCredentials(TOKEN).toString()).doesNotContain(TOKEN);
    assertThat(
            new RequestContext("tenant-1", "case-1", "org-1", "alice", "corr-1", TOKEN).toString())
        .doesNotContain(TOKEN)
        .contains("accessTokenPresent=true");
    assertThat(
            new SessionContext("alice", "tenant-1", TOKEN, "refresh-secret", "{}", "fp").toString())
        .doesNotContain(TOKEN)
        .doesNotContain("refresh-secret");
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"", "  "})
  void bearerCredentials_refuseToBuildAnEmptyBearerHeader(String accessToken) {
    assertThatThrownBy(() -> new BearerTokenCallCredentials(accessToken))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void historyTurnWithNullOrBlankContent_isSentAsIs_withNullMappedToEmpty() throws IOException {
    MlAgentRequest requestWithEmptyContent =
        new MlAgentRequest(
            "tenant-1",
            "org-1",
            "case-1",
            List.of(
                new MlAgentRequest.HistoryTurn("user", null),
                new MlAgentRequest.HistoryTurn("assistant", null),
                new MlAgentRequest.HistoryTurn("user", "  ")),
            "msg-1",
            "analyst-1",
            null,
            "corr-1",
            "req-1",
            "hi",
            null);
    GrpcMlAgentClient client = startStreamingClient();

    StepVerifier.create(client.streamResponse(requestWithEmptyContent))
        .expectNext(CHUNK, DONE)
        .verifyComplete();

    AskCaseManagerRequest sent = capturedRequest.get();
    assertThat(sent.getHistory(0).getUser().getPrompt()).isEmpty();
    assertThat(sent.getHistory(1).getAgent().getText()).isEmpty();
    assertThat(sent.getHistory(2).getUser().getPrompt()).isEqualTo("  ");
  }
}
