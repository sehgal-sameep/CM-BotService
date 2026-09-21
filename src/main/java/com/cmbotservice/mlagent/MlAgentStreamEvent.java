package com.cmbotservice.mlagent;

/**
 * One signal from an {@link MlAgentClient#streamResponse} {@code Flux}. A sealed interface rather
 * than a callback: errors are deliberately <b>not</b> a variant here — they flow through the {@code
 * Flux}'s native error channel, which is what lets {@code .timeout()}, {@code .retryWhen()}, and
 * the resilience4j operators compose declaratively in {@code ChatOrchestrationService} instead of
 * needing a hand-rolled state machine. Consumers pattern-match exhaustively over the six variants
 * below; the compiler enforces that every case is handled.
 *
 * <p>The real ML Agent's own {@code tool_call}/{@code tool_result} events are surfaced here as
 * {@link ToolCall}/{@link ToolResult} — forwarded on to the frontend as trace events (see {@code
 * ChatOrchestrationService}/{@code com.cmbotservice.sse}) rather than only logged, so case managers
 * can see what the agent did to produce an answer. {@code ping} is a transport-level keepalive with
 * no content and remains logged only — {@code GrpcMlAgentClient}/{@link MockMlAgentClient} consume
 * it directly and never turn it into a domain event; nothing downstream of this interface is meant
 * to see it.
 *
 * <p>Notably, {@link Started} carries no conversation identifier: the real contract has no
 * conversation/continuation identifier at all — {@code history} on the next request is the sole
 * resumption mechanism.
 */
public sealed interface MlAgentStreamEvent {

  /**
   * The ML Agent has accepted the request and will begin streaming. Carries nothing — purely a "the
   * call is underway" marker for logging/metrics timing.
   */
  record Started() implements MlAgentStreamEvent {}

  /**
   * One streamed fragment of the answer text (the real contract's {@code chunk} event, field {@code
   * delta}). {@code sequence} is 1-based and strictly increasing.
   */
  record Token(String delta, int sequence) implements MlAgentStreamEvent {}

  /**
   * The ML Agent invoked a tool (the real contract's {@code tool_call} event, fields {@code
   * tool_call_id}/{@code name}/{@code args_json}). May arrive at any point in the stream, zero or
   * more times; {@code toolCallId} matches the corresponding {@link ToolResult}.
   */
  record ToolCall(String toolCallId, String name, String argsJson) implements MlAgentStreamEvent {}

  /**
   * The result of a tool invocation (the real contract's {@code tool_result} event). {@code
   * toolCallId} matches the {@link ToolCall} that triggered it. {@code rowCount} mirrors the
   * contract's {@code result_summary} oneof and is {@code null} when that oneof is unset (e.g. on
   * failure, or a future summary type this backend doesn't carry a field for yet).
   */
  record ToolResult(String toolCallId, Status status, long ms, Long rowCount)
      implements MlAgentStreamEvent {

    /**
     * Mirrors the contract's {@code ToolResult.Status} enum, minus {@code STATUS_UNSPECIFIED} — per
     * the contract, "Clients MUST treat any unrecognised value as STATUS_FAILED", so {@code
     * GrpcMlAgentClient} folds unspecified/unrecognised values into {@link #FAILED} rather than
     * propagating a third state here.
     */
    public enum Status {
      OK,
      FAILED
    }
  }

  /**
   * The structured, cited case analysis (the real contract's {@code payload} event) — exactly one
   * per response, always before {@link Done}.
   */
  record Payload(CaseSummaryPayload payload) implements MlAgentStreamEvent {}

  /**
   * The response is complete; no further events will follow. Carries usage/latency figures for
   * observability, plus whether generation was cut short ({@code truncated}, from the real
   * contract's {@code stop_reason}) — the answer text received so far is still coherent and MUST be
   * persisted, just incomplete.
   */
  record Done(long latencyMs, long tokensIn, long tokensOut, boolean truncated)
      implements MlAgentStreamEvent {}
}
