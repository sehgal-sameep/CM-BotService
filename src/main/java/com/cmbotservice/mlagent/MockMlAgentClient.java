package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.CaseManagerAnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.Chunk;
import com.cmbotservice.mlagent.grpc.v1.Done;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.ToolCall;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Stand-in for the real ML Agent, used until the ML team's API is available.
 *
 * <p>Fully reactive and cold: nothing runs — no thread, no timer — until the returned {@code Flux}
 * is subscribed, and cancelling that subscription (e.g. the orchestrator's timeout firing, or a
 * client disconnect) stops everything downstream of it for free via Reactor's own cancellation
 * propagation. Behavior is selected via {@link MockScenario} keywords in the message text so every
 * failure mode the real integration must handle is reachable from Swagger/curl with plain text,
 * without polluting the request contract with mock-only fields.
 *
 * <p>Emits the real contract's own {@link AnswerEvent} messages — the exact type {@code
 * GrpcMlAgentClient} relays — so what the frontend receives in mock mode is byte-for-byte the shape
 * it will receive from the real agent: a {@code tool_call}/{@code tool_result} trace, {@code chunk}
 * events, a {@code payload} (with a citation on every key signal), then {@code done}. Transport
 * failures ({@code trigger:error}/{@code trigger:rejected}) are {@code Flux} errors, exactly as
 * {@code GrpcMlAgentClient} surfaces a gRPC status; {@code trigger:agent-error} instead emits the
 * agent's own model-level {@code error} event.
 *
 * <p>Active whenever {@code ml-agent.mode} is {@code mock} (the default).
 */
@Component
@ConditionalOnProperty(
    prefix = "ml-agent",
    name = "mode",
    havingValue = "mock",
    matchIfMissing = true)
@Slf4j
public class MockMlAgentClient implements MlAgentClient {

  private static final String SUMMARY_RESPONSE =
      "This case was created because the transaction triggered multiple fraud "
          + "indicators. The transaction amount was significantly higher than the "
          + "customer's typical spending pattern. It also originated from a device "
          + "and IP address not previously associated with this account. Two "
          + "velocity rules and one geolocation rule were triggered as a result. "
          + "The case is currently awaiting analyst review.";

  private static final String RULES_RESPONSE =
      "Two rules were triggered on this transaction. The first is a velocity rule, "
          + "flagging an unusual number of transactions in a short time window. The "
          + "second is a geolocation rule, flagging a mismatch between the "
          + "transaction origin and the customer's known location history.";

  private static final String SUSPICION_RESPONSE =
      "The transaction was considered suspicious primarily due to its deviation "
          + "from the customer's established behavior profile. The amount, "
          + "merchant category, and originating device were all atypical for this "
          + "account. Combined, these factors raised the transaction's risk score "
          + "above the case-creation threshold.";

  private static final String NEXT_STEPS_RESPONSE =
      "A reasonable next step is to verify the transaction directly with the "
          + "customer through an out-of-band channel. It is also worth reviewing "
          + "recent account activity for other atypical transactions, and checking "
          + "whether the device or IP address has appeared on prior cases.";

  private static final String GENERIC_RESPONSE =
      "Based on the information available for this case, the transaction shows "
          + "several characteristics consistent with fraudulent activity. Further "
          + "review of the linked account and rule history is recommended before "
          + "reaching a final disposition.";

  private final Duration chunkDelay;
  private final Duration slowChunkDelay;

  public MockMlAgentClient(
      @Value("${app.mock-ml-agent.chunk-delay-ms}") long chunkDelayMs,
      @Value("${app.mock-ml-agent.slow-chunk-delay-ms}") long slowChunkDelayMs) {
    this.chunkDelay = Duration.ofMillis(chunkDelayMs);
    this.slowChunkDelay = Duration.ofMillis(slowChunkDelayMs);
    log.info(
        "ML_AGENT_MOCK_CONFIGURED ml-agent.mode=mock — no real ML Agent will be called"
            + " chunkDelayMs={} slowChunkDelayMs={}",
        chunkDelayMs,
        slowChunkDelayMs);
  }

  @Override
  public Flux<AnswerEvent> streamResponse(MlAgentRequest request) {
    MockScenario scenario = MockScenario.fromPrompt(request.message());
    log.info("MOCK_ML_AGENT_CALL_STARTED messageId={} scenario={}", request.messageId(), scenario);

    return switch (scenario) {
      case ERROR ->
          Flux.error(
              new MlAgentCommunicationException("Simulated ML Agent failure (trigger:error)"));
      case REJECTED ->
          Flux.error(
              new MlAgentRejectedException(
                  ErrorCode.NOT_FOUND,
                  "Simulated rejection: case not found in that tenant (trigger:rejected)"));
      case AGENT_ERROR ->
          Flux.just(
              AnswerEvent.newBuilder()
                  .setError(
                      Error.newBuilder()
                          .setCode(Error.Code.ERROR_CODE_DATA_UNAVAILABLE)
                          .setRetryable(true))
                  .build());
      case EMPTY -> Flux.just(done(0, 0, 0));
      // Deliberately never emits — the orchestrator's own first-response/idle-timeout
      // operator is what ends this stream; the mock has no timer of its own.
      case TIMEOUT -> Flux.never();
      case SLOW -> streamChunks(request, selectCannedResponse(request.message()), slowChunkDelay);
      default -> streamChunks(request, selectCannedResponse(request.message()), chunkDelay);
    };
  }

  private Flux<AnswerEvent> streamChunks(MlAgentRequest request, String text, Duration delay) {
    List<String> sentences = splitIntoSentences(text);
    Flux<AnswerEvent> chunks =
        Flux.fromIterable(sentences)
            .delayElements(delay)
            .map(
                sentence ->
                    AnswerEvent.newBuilder()
                        .setChunk(Chunk.newBuilder().setDelta(sentence))
                        .build());
    long approxTokensIn = text == null ? 0 : Math.max(1, text.length() / 4);
    long approxTokensOut = sentences.size() * 10L;
    long approxLatencyMs = sentences.size() * delay.toMillis();
    return Flux.concat(
        Flux.fromIterable(toolTrace(request.caseId())),
        chunks,
        Mono.just(AnswerEvent.newBuilder().setPayload(buildPayload()).build()),
        Mono.just(done(approxLatencyMs, approxTokensIn, approxTokensOut)));
  }

  private static List<AnswerEvent> toolTrace(String caseId) {
    String toolCallId = "mock-tool-" + UUID.randomUUID().toString().substring(0, 8);
    return List.of(
        AnswerEvent.newBuilder()
            .setToolCall(
                ToolCall.newBuilder()
                    .setToolCallId(toolCallId)
                    .setName("getCase")
                    .setArgsJson(
                        caseId == null ? "{\"caseId\":null}" : "{\"caseId\":\"" + caseId + "\"}"))
            .build(),
        AnswerEvent.newBuilder()
            .setToolResult(
                ToolResult.newBuilder()
                    .setToolCallId(toolCallId)
                    .setStatus(ToolResult.Status.STATUS_OK)
                    .setMs(42)
                    .setRowCount(1))
            .build());
  }

  private static AnswerEvent done(long latencyMs, long tokensIn, long tokensOut) {
    return AnswerEvent.newBuilder()
        .setDone(
            Done.newBuilder()
                .setStopReason(Done.StopReason.STOP_REASON_COMPLETED)
                .setLatencyMs(latencyMs)
                .setTokensIn(tokensIn)
                .setTokensOut(tokensOut))
        .build();
  }

  /** Every key signal carries a citation, as the real contract requires. */
  private static AnswerPayload buildPayload() {
    String citationId =
        "MOCK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);
    return AnswerPayload.newBuilder()
        .setCaseManagerAnswerPayload(
            CaseManagerAnswerPayload.newBuilder()
                .addKeySignals(
                    CaseManagerAnswerPayload.KeySignal.newBuilder()
                        .setSignal(
                            "Transaction deviates from the customer's typical behavior profile")
                        .addCitations(citationId))
                .addCitations(
                    CaseManagerAnswerPayload.Citation.newBuilder()
                        .setId(citationId)
                        .setSource("getCase")
                        .addFields("risk_score")))
        .build();
  }

  private static List<String> splitIntoSentences(String text) {
    return List.of(text.split("(?<=[.])\\s+"));
  }

  private static String selectCannedResponse(String message) {
    String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
    if (lower.contains("summar")) {
      return SUMMARY_RESPONSE;
    }
    if (lower.contains("rule")) {
      return RULES_RESPONSE;
    }
    if (lower.contains("suspicious") || lower.contains("unusual")) {
      return SUSPICION_RESPONSE;
    }
    if (lower.contains("next") || lower.contains("investigate")) {
      return NEXT_STEPS_RESPONSE;
    }
    return GENERIC_RESPONSE;
  }
}
