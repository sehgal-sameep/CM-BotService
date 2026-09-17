package com.cmbotservice.context;

import org.springframework.web.server.ServerWebExchange;

/**
 * Resolves "who is calling, for which tenant/organization/case" into a
 * {@link RequestContext}. {@code tenantId}/{@code organization} are read from the
 * {@code X-Tenant-Id}/{@code X-Org-Id} request headers (see
 * {@link RequestHeaders}) by every implementation — both are required, and an
 * implementation rejects the request (400, {@code ErrorCode.VALIDATION_ERROR}) if
 * either is missing or blank.
 * <p>
 * This was the single seam real authentication plugged in at: {@link HeaderBasedRequestContextResolver}
 * (active in {@code chatbot.security.mode: NONE}, trusting an {@code X-User-Id} header)
 * and {@code com.cmbotservice.security.SessionRequestContextResolver} (active in
 * {@code mode: BFF_SESSION}, backed by a validated session) are selected purely by
 * that one property — no controller or service code change either way, since they
 * only ever consume the resolved {@link RequestContext}.
 */
public interface RequestContextResolver {

    RequestContext resolve(ServerWebExchange exchange, String caseId);
}
