package com.cmbotservice.mlagent;

/**
 * Everything the ML Agent needs to answer a single analyst message, plus a few
 * internal-only fields ({@code messageId}/{@code correlationId}/{@code requestId})
 * used for our own logging/tracing. This is the boundary contract
 * between our orchestration logic and whatever the real ML Agent turns out to be — it
 * must remain stable across {@link MlAgentClient} implementations.
 * <p>
 * This is <b>not</b> the literal wire body sent to the real ML Agent — the real
 * {@code POST /v1/chat} contract only wants a subset of these fields, shaped
 * differently (a nested {@code context} object, an {@code options} object, etc.).
 * {@link HttpMlAgentClient} owns that translation; this record is the stable internal
 * contract every {@link MlAgentClient} implementation (mock or real) is built against.
 * <p>
 * This backend is a stateless pass-through: it does not assemble or send conversation
 * history. Follow-up turns need prior context via exactly one of {@code continuation}
 * or {@code conversationId} (the real contract's third option, an explicit
 * {@code history} transcript, is "eval only" and would require storing/replaying
 * messages — directly against this service's stateless design, so it's never used).
 * Per the real contract's own guidance, this service doesn't need to choose between
 * {@code continuation}/{@code conversationId} — it just echoes back whatever the
 * previous {@link MlAgentStreamEvent.Done} returned, verbatim.
 * <p>
 * {@code endUserId} is an optional hint whose exact semantics (the analyst vs. the
 * case's customer) aren't yet pinned down upstream — forwarded as-is, never
 * interpreted here. {@code includeResolutions} controls whether the agent includes a
 * {@code suggestedResolution} in its {@code payload} event.
 */
public record MlAgentRequest(
        String tenantId,
        String caseId,
        String continuation,
        String conversationId,
        String messageId,
        String userId,
        String endUserId,
        String correlationId,
        String requestId,
        String surface,
        boolean includeResolutions,
        String message
) {

    /**
     * The only {@code surface} value this backend ever sends — it is, definitionally,
     * the Case Manager chatbot backend. {@code policy_manager} is a different product
     * surface the real contract also supports, not something this service produces.
     */
    public static final String SURFACE_CASE_MANAGER = "case_manager";
}
