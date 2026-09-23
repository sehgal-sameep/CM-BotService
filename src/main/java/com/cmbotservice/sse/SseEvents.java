package com.cmbotservice.sse;

import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.springframework.http.codec.ServerSentEvent;

/**
 * Builds the {@code ServerSentEvent}s Spring writes to the response. A pure mapping function — no
 * state, no I/O. Frame numbering ({@code id:}) is applied once, upstream, via {@code Flux#index()}
 * in {@code ChatOrchestrationService} and handed to {@link #withId}.
 *
 * <p>An ML Agent {@link AnswerEvent} is relayed verbatim:
 *
 * <ul>
 *   <li>{@code event:} is the name of the {@code AnswerEvent} oneof field that is set ({@code
 *       chunk}, {@code tool_call}, {@code tool_result}, {@code payload}, {@code done}, {@code
 *       error}, {@code ping}), read from the protobuf descriptor — never a name of our own.
 *   <li>{@code data:} is the whole {@code AnswerEvent} in the canonical proto3 JSON mapping, with
 *       the original {@code .proto} field names preserved ({@code tool_call_id}, not {@code
 *       toolCallId}), the original nesting (e.g. {@code
 *       {"payload":{"case_manager_answer_payload":{...}}}}), enum values as their proto names
 *       ({@code STATUS_OK}), and fields at their default value still printed rather than omitted,
 *       so no field is ever silently missing. Per that mapping, {@code int64} values ({@code ms},
 *       {@code row_count}, {@code latency_ms}, {@code tokens_in}, {@code tokens_out}) are JSON
 *       strings.
 * </ul>
 */
public final class SseEvents {

  private static final JsonFormat.Printer ANSWER_EVENT_PRINTER =
      JsonFormat.printer()
          .preservingProtoFieldNames()
          .alwaysPrintFieldsWithNoPresence()
          .omittingInsignificantWhitespace();

  private SseEvents() {}

  /**
   * The ML Agent's event, unchanged. The {@code data} is handed to Spring as a pre-rendered JSON
   * {@code String}, which its SSE writer emits as-is (no second JSON encoding).
   */
  public static ServerSentEvent<Object> fromAnswerEvent(AnswerEvent event) {
    return ServerSentEvent.<Object>builder().event(eventName(event)).data(toJson(event)).build();
  }

  /** A failure with no ML Agent event of its own to forward — see {@link ServiceErrorEvent}. */
  public static ServerSentEvent<Object> fromServiceError(ServiceErrorEvent error) {
    return ServerSentEvent.<Object>builder().event(SseEventType.SERVICE_ERROR).data(error).build();
  }

  public static ServerSentEvent<Object> withId(long frameId, ServerSentEvent<Object> event) {
    return ServerSentEvent.builder(event.data())
        .id(String.valueOf(frameId))
        .event(event.event())
        .build();
  }

  /** The proto field name of the set {@code event} oneof arm, e.g. {@code tool_call}. */
  public static String eventName(AnswerEvent event) {
    return AnswerEvent.getDescriptor()
        .findFieldByNumber(event.getEventCase().getNumber())
        .getName();
  }

  public static String toJson(AnswerEvent event) {
    try {
      return ANSWER_EVENT_PRINTER.print(event);
    } catch (InvalidProtocolBufferException e) {
      throw new MlAgentMalformedResponseException(
          "ML Agent '" + eventName(event) + "' event could not be rendered as JSON", e);
    }
  }
}
