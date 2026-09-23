package com.cmbotservice.sse;

import static org.assertj.core.api.Assertions.assertThat;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.cmbotservice.mlagent.grpc.v1.AnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.CaseManagerAnswerPayload;
import com.cmbotservice.mlagent.grpc.v1.Chunk;
import com.cmbotservice.mlagent.grpc.v1.Done;
import com.cmbotservice.mlagent.grpc.v1.Error;
import com.cmbotservice.mlagent.grpc.v1.Ping;
import com.cmbotservice.mlagent.grpc.v1.ToolCall;
import com.cmbotservice.mlagent.grpc.v1.ToolResult;
import com.google.protobuf.util.JsonFormat;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.codec.ServerSentEvent;

/**
 * Pins the frontend-facing wire format: each ML Agent {@link AnswerEvent} becomes one SSE event
 * whose name is the agent's own oneof field name and whose data is the agent's own message in
 * canonical proto3 JSON — no renamed fields, no renamed events, no reshaping, nothing dropped.
 */
class SseEventsTest {

  private static final AnswerEvent CHUNK =
      AnswerEvent.newBuilder().setChunk(Chunk.newBuilder().setDelta("Hello")).build();

  private static final AnswerEvent TOOL_CALL =
      AnswerEvent.newBuilder()
          .setToolCall(
              ToolCall.newBuilder()
                  .setToolCallId("t-1")
                  .setName("getCase")
                  .setArgsJson("{\"caseId\":\"case-1\"}"))
          .build();

  private static final AnswerEvent TOOL_RESULT =
      AnswerEvent.newBuilder()
          .setToolResult(
              ToolResult.newBuilder()
                  .setToolCallId("t-1")
                  .setStatus(ToolResult.Status.STATUS_OK)
                  .setMs(42)
                  .setRowCount(7))
          .build();

  private static final AnswerEvent PAYLOAD =
      AnswerEvent.newBuilder()
          .setPayload(
              AnswerPayload.newBuilder()
                  .setCaseManagerAnswerPayload(
                      CaseManagerAnswerPayload.newBuilder()
                          .addKeySignals(
                              CaseManagerAnswerPayload.KeySignal.newBuilder()
                                  .setSignal("Unusual device/IP")
                                  .addCitations("evt-123"))
                          .addCitations(
                              CaseManagerAnswerPayload.Citation.newBuilder()
                                  .setId("evt-123")
                                  .setSource("getCase")
                                  .addFields("risk_score"))))
          .build();

  private static final AnswerEvent DONE =
      AnswerEvent.newBuilder()
          .setDone(
              Done.newBuilder()
                  .setStopReason(Done.StopReason.STOP_REASON_COMPLETED)
                  .setLatencyMs(1800)
                  .setTokensIn(12)
                  .setTokensOut(140))
          .build();

  private static final AnswerEvent ERROR =
      AnswerEvent.newBuilder()
          .setError(
              Error.newBuilder().setCode(Error.Code.ERROR_CODE_MODEL_REFUSED).setRetryable(false))
          .build();

  private static final AnswerEvent PING =
      AnswerEvent.newBuilder().setPing(Ping.newBuilder()).build();

  @Test
  void eventName_isTheAgentsOwnOneofFieldName_forEveryEventType() {
    assertThat(SseEvents.fromAnswerEvent(CHUNK).event()).isEqualTo("chunk");
    assertThat(SseEvents.fromAnswerEvent(TOOL_CALL).event()).isEqualTo("tool_call");
    assertThat(SseEvents.fromAnswerEvent(TOOL_RESULT).event()).isEqualTo("tool_result");
    assertThat(SseEvents.fromAnswerEvent(PAYLOAD).event()).isEqualTo("payload");
    assertThat(SseEvents.fromAnswerEvent(DONE).event()).isEqualTo("done");
    assertThat(SseEvents.fromAnswerEvent(ERROR).event()).isEqualTo("error");
    assertThat(SseEvents.fromAnswerEvent(PING).event()).isEqualTo("ping");
  }

  @Test
  void everyOneofArmOfTheContract_hasANameDerivedFromTheDescriptor() {
    // Guards against a new arm being added to chat_agent.proto without flowing through:
    // every oneof field is exercised by name, none via a hand-written mapping.
    List<String> contractArms =
        AnswerEvent.getDescriptor().getOneofs().getFirst().getFields().stream()
            .map(f -> f.getName())
            .toList();
    assertThat(contractArms)
        .containsExactly("chunk", "tool_call", "tool_result", "payload", "done", "error", "ping");
  }

  @Test
  void data_keepsTheAgentsFieldNamesNestingAndEnumNames_untranslated() {
    assertThat(data(CHUNK)).isEqualTo("{\"chunk\":{\"delta\":\"Hello\"}}");
    assertThat(data(TOOL_CALL))
        .isEqualTo(
            "{\"tool_call\":{\"tool_call_id\":\"t-1\",\"name\":\"getCase\","
                + "\"args_json\":\"{\\\"caseId\\\":\\\"case-1\\\"}\"}}");
    assertThat(data(TOOL_RESULT))
        .isEqualTo(
            "{\"tool_result\":{\"tool_call_id\":\"t-1\",\"status\":\"STATUS_OK\","
                + "\"ms\":\"42\",\"row_count\":\"7\"}}");
    assertThat(data(PAYLOAD))
        .isEqualTo(
            "{\"payload\":{\"case_manager_answer_payload\":{"
                + "\"key_signals\":[{\"signal\":\"Unusual device/IP\",\"citations\":[\"evt-123\"]}],"
                + "\"citations\":[{\"id\":\"evt-123\",\"source\":\"getCase\","
                + "\"fields\":[\"risk_score\"]}]}}}");
    assertThat(data(DONE))
        .isEqualTo(
            "{\"done\":{\"stop_reason\":\"STOP_REASON_COMPLETED\",\"latency_ms\":\"1800\","
                + "\"tokens_in\":\"12\",\"tokens_out\":\"140\"}}");
    assertThat(data(ERROR))
        .isEqualTo("{\"error\":{\"code\":\"ERROR_CODE_MODEL_REFUSED\",\"retryable\":false}}");
    assertThat(data(PING)).isEqualTo("{\"ping\":{}}");
  }

  @Test
  void data_neverDropsFieldsThatHoldTheirDefaultValue() {
    AnswerEvent allDefaults =
        AnswerEvent.newBuilder()
            .setToolResult(ToolResult.newBuilder()) // status UNSPECIFIED, ms 0, no row_count
            .build();

    assertThat(data(allDefaults))
        .isEqualTo(
            "{\"tool_result\":{\"tool_call_id\":\"\",\"status\":\"STATUS_UNSPECIFIED\",\"ms\":\"0\"}}");
  }

  @Test
  void data_roundTripsBackToTheIdenticalProtoMessage_forEveryEventType() throws Exception {
    for (AnswerEvent original :
        List.of(CHUNK, TOOL_CALL, TOOL_RESULT, PAYLOAD, DONE, ERROR, PING)) {
      AnswerEvent.Builder parsed = AnswerEvent.newBuilder();
      JsonFormat.parser().merge(data(original), parsed);
      assertThat(parsed.build())
          .as("round trip of %s", original.getEventCase())
          .isEqualTo(original);
    }
  }

  @Test
  void serviceError_isTheOnlyBackendDefinedEvent_andUsesItsOwnDistinctName() {
    ServiceErrorEvent error =
        new ServiceErrorEvent("m-1", ErrorCode.ML_AGENT_TIMEOUT, "timed out", Instant.EPOCH);

    ServerSentEvent<Object> sse = SseEvents.fromServiceError(error);

    assertThat(sse.event()).isEqualTo("service_error").isNotEqualTo("error");
    assertThat(sse.data()).isEqualTo(error);
  }

  @Test
  void withId_addsOnlyTheTransportFrameId_leavingNameAndDataUntouched() {
    ServerSentEvent<Object> original = SseEvents.fromAnswerEvent(TOOL_CALL);

    ServerSentEvent<Object> numbered = SseEvents.withId(3, original);

    assertThat(numbered.id()).isEqualTo("3");
    assertThat(numbered.event()).isEqualTo(original.event());
    assertThat(numbered.data()).isEqualTo(original.data());
  }

  private static String data(AnswerEvent event) {
    Object data = SseEvents.fromAnswerEvent(event).data();
    assertThat(data).isInstanceOf(String.class);
    return (String) data;
  }
}
