package com.cmbotservice.mlagent;

import java.util.List;

/**
 * Everything the ML Agent needs to answer a single analyst message, plus a few
 * internal-only fields ({@code messageId}/{@code correlationId}/{@code requestId})
 * used for our own logging/tracing. This is the boundary contract
 * between our orchestration logic and whatever the real ML Agent turns out to be — it
 * must remain stable across {@link MlAgentClient} implementations.
 * <p>
 * This is <b>not</b> the literal wire message sent to the real ML Agent — the real
 * {@code Chat} RPC only wants a subset of these fields, shaped differently (a nested
 * {@code context} message, an {@code options} message, etc.). {@code GrpcMlAgentClient}
 * owns that translation; this record is the stable internal contract every
 * {@link MlAgentClient} implementation (mock or real) is built against.
 * <p>
 * This backend remains a stateless pass-through even though it now forwards
 * {@code history}: it never assembles, stores, or replays a transcript itself — the
 * caller (frontend/BFF) owns remembering and resending {@code history},
 * {@code continuation}, and {@code conversationId} alike, exactly as
 * {@link com.cmbotservice.web.dto.ChatRequest} received them. Per the real contract's
 * own guidance (precedence {@code history > continuation > conversationId}), this
 * service never chooses between the three — it just forwards whatever the caller
 * supplied, and echoes back whatever the previous {@link MlAgentStreamEvent.Done}
 * returned for {@code continuation}/{@code conversationId}, verbatim.
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
        List<HistoryTurn> history,
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

    /**
     * One turn of an explicit conversation transcript, forwarded to the ML Agent
     * untouched. The real contract documents {@code history} only as "an explicit
     * transcript" with no field-level schema given — {@code role}/{@code content} is
     * this backend's best-effort assumption (the de facto standard shape for a chat
     * transcript), not a confirmed part of the contract; see
     * {@code src/main/proto/chat_agent.proto}'s {@code HistoryTurn} message and
     * README.md "Known limitations".
     */
    public record HistoryTurn(String role, String content) {
    }
}
