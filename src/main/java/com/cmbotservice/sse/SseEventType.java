package com.cmbotservice.sse;

/**
 * The only SSE {@code event:} name this backend defines itself. Every other event name on the
 * stream is the ML Agent's own {@code AnswerEvent} oneof field name ({@code chunk}, {@code
 * tool_call}, {@code tool_result}, {@code payload}, {@code done}, {@code error}, {@code ping}),
 * read from the generated protobuf descriptor at runtime by {@link SseEvents} rather than
 * hard-coded here, so a new arm added to the contract flows through with its own name unchanged.
 */
public final class SseEventType {

  /**
   * A failure originating in this backend or its transport to the ML Agent — see {@link
   * ServiceErrorEvent}. Deliberately distinct from the ML Agent's own {@code error} event name,
   * which carries a different payload.
   */
  public static final String SERVICE_ERROR = "service_error";

  private SseEventType() {}
}
