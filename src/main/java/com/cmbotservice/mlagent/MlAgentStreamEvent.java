package com.cmbotservice.mlagent;

/**
 * One signal from an {@link MlAgentClient#streamResponse} {@code Flux}. A sealed
 * interface rather than a callback: errors are deliberately <b>not</b> a variant here
 * — they flow through the {@code Flux}'s native error channel, which is what lets
 * {@code .timeout()}, {@code .retryWhen()}, and the resilience4j operators compose
 * declaratively in {@code ChatOrchestrationService} instead of needing a hand-rolled
 * state machine. Consumers pattern-match exhaustively over the four variants below;
 * the compiler enforces that every case is handled.
 * <p>
 * The real ML Agent's own {@code tool_call}/{@code tool_result} events are
 * deliberately <b>not</b> represented here at all — per the contract, they're
 * "rendered in the sandbox trace, logged in product." This service is the product,
 * not the sandbox, so {@link HttpMlAgentClient}/{@link MockMlAgentClient} consume and
 * log them directly and never turn them into a domain event; nothing downstream of
 * this interface is meant to see them.
 * <p>
 * Notably, {@link Started} carries no conversation identifier: the real agent never
 * reveals one until the final {@link Done} event. An earlier, placeholder version of
 * this contract assumed the agent assigned one up front — it doesn't, so there is
 * nothing to hand back until the stream actually finishes.
 */
public sealed interface MlAgentStreamEvent {

    /**
     * The ML Agent has accepted the request and will begin streaming. Carries nothing
     * — purely a "the call is underway" marker for logging/metrics timing.
     */
    record Started() implements MlAgentStreamEvent {
    }

    /**
     * One streamed fragment of the answer text (the real contract's {@code token}
     * event, field {@code delta}). {@code sequence} is 1-based and strictly increasing.
     */
    record Token(String delta, int sequence) implements MlAgentStreamEvent {
    }

    /**
     * The structured, cited case analysis (the real contract's {@code payload}
     * event) — exactly one per response, always before {@link Done}.
     */
    record Payload(CaseSummaryPayload payload) implements MlAgentStreamEvent {
    }

    /**
     * The response is complete; no further events will follow. Carries the
     * authoritative conversation identifiers — this is the only point at which the
     * real agent reveals them — plus usage/latency figures for observability.
     */
    record Done(String conversationId, String continuation, long latencyMs, long tokensIn, long tokensOut)
            implements MlAgentStreamEvent {
    }
}
