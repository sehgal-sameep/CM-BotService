package com.cmbotservice.mlagent;

import java.util.List;

/**
 * Everything the ML Agent needs to answer a single analyst message, plus a few internal-only fields
 * ({@code messageId}/{@code correlationId}) used for our own logging/tracing. This is the boundary
 * contract between our orchestration logic and whatever the real ML Agent turns out to be — it must
 * remain stable across {@link MlAgentClient} implementations.
 *
 * <p>This is <b>not</b> the literal wire message sent to the real ML Agent — the real {@code
 * AskCaseManager} RPC only wants a subset of these fields, shaped differently (a nested {@code
 * request_context} message, a nested {@code case_context} message, etc.). {@code GrpcMlAgentClient}
 * owns that translation; this record is the stable internal contract every {@link MlAgentClient}
 * implementation (mock or real) is built against.
 *
 * <p>This backend is a fully stateless pass-through: the real contract's only resumption mechanism
 * is the caller resending the full {@code history} on every request — there is no continuation
 * token or conversation id to echo back, and this service never assembles, stores, or replays a
 * transcript itself.
 *
 * <p>{@code endUserId} is an optional hint whose exact semantics (the analyst vs. the case's
 * customer) aren't yet pinned down upstream — forwarded as-is, never interpreted here. {@code
 * operatorId} identifies the analyst making the request.
 */
public record MlAgentRequest(
    String tenantId,
    String organization,
    String caseId,
    List<HistoryTurn> history,
    String messageId,
    String operatorId,
    String endUserId,
    String correlationId,
    String requestId,
    String message) {

  /**
   * One turn of an explicit conversation transcript, forwarded to the ML Agent untouched. The real
   * contract's {@code ConversationTurn} is a {@code user}/ {@code agent} oneof rather than a {@code
   * role}/{@code content} pair — {@code GrpcMlAgentClient} owns translating {@code role} into the
   * right oneof arm; this shape is this backend's own stable internal representation, not the wire
   * format.
   */
  public record HistoryTurn(String role, String content) {}
}
