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
 * source it from) and the BFF-issued {@code access_token} in {@code mode: BFF_SESSION} — carried
 * through here, not yet forwarded anywhere, so a future call to the TFLabs Orchestrator Service has
 * it available without another Redis round-trip.
 */
public record RequestContext(
    String tenantId,
    String caseId,
    String organization,
    String userId,
    String correlationId,
    String accessToken) {}
