package com.cmbotservice.context;

/**
 * Identity/scoping context for a single request: who is calling, and for which
 * tenant/organization/case. Resolved once per request by a {@link RequestContextResolver} and
 * threaded explicitly through service calls (deliberately not stored in a ThreadLocal or ambient
 * holder) so tenant/case scoping is compiler-checked at every boundary and cannot be silently
 * dropped or leaked across an executor hop.
 *
 * <p>{@code tenantId}/{@code organization} come from the {@code X-Tenant-Id}/ {@code X-Org-Id}
 * request headers (see {@link RequestHeaders}), not the request body — both are required on every
 * request.
 *
 * <p>{@code accessToken} is {@code null} in {@code chatbot.security.mode: NONE} (no BFF session to
 * source it from) and the BFF-issued {@code access_token} in {@code mode: BFF_SESSION}. It is
 * forwarded to the ML Agent only as {@code authorization: Bearer} gRPC call metadata (see {@code
 * GrpcMlAgentClient}); {@link #toString()} redacts it so it can't reach a log line by accident.
 */
public record RequestContext(
    String tenantId,
    String caseId,
    String organization,
    String userId,
    String correlationId,
    String accessToken) {

  @Override
  public String toString() {
    return "RequestContext[tenantId=%s, caseId=%s, organization=%s, userId=%s, correlationId=%s,"
            .formatted(tenantId, caseId, organization, userId, correlationId)
        + " accessTokenPresent=%s]".formatted(accessToken != null && !accessToken.isBlank());
  }
}
