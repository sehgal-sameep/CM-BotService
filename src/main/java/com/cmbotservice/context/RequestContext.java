package com.cmbotservice.context;

/**
 * Identity/scoping context for a single request: who is calling, and for which
 * tenant/case. Resolved once per request by a {@link RequestContextResolver} and
 * threaded explicitly through service calls (deliberately not stored in a ThreadLocal
 * or ambient holder) so tenant/case scoping is compiler-checked at every boundary and
 * cannot be silently dropped or leaked across an executor hop.
 */
public record RequestContext(
        String tenantId,
        String caseId,
        String userId,
        String correlationId
) {
}
