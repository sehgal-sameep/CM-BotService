package com.cmbotservice.context;

import org.springframework.web.server.ServerWebExchange;

/**
 * Resolves "who is calling, for which tenant/case" into a {@link RequestContext}.
 * <p>
 * This is the single seam where real authentication will plug in later: swap the
 * {@link HeaderBasedRequestContextResolver} bean for one backed by a JWT/session
 * principal, and no controller or service code changes, since they only ever consume
 * the resolved {@link RequestContext}.
 */
public interface RequestContextResolver {

    RequestContext resolve(ServerWebExchange exchange, String tenantId, String caseId);
}
