package com.cmbotservice.context;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

/**
 * {@code chatbot.security.mode: NONE} {@link RequestContextResolver}: trusts an {@code X-User-Id}
 * header for analyst identity, and the {@code X-Tenant-Id}/ {@code X-Org-Id} headers for
 * tenant/organization scoping. Not a real authentication mechanism — local development only, active
 * whenever authentication is bypassed. See {@link
 * com.cmbotservice.security.SessionRequestContextResolver} for the {@code BFF_SESSION}
 * implementation that replaces this one, with no change required to any consumer of {@link
 * RequestContext}.
 */
@Component
@ConditionalOnProperty(
    prefix = "chatbot.security",
    name = "mode",
    havingValue = "NONE",
    matchIfMissing = true)
public class HeaderBasedRequestContextResolver implements RequestContextResolver {

  private static final String UNKNOWN_USER = "unknown-user";

  @Override
  public RequestContext resolve(ServerWebExchange exchange, String caseId) {
    String tenantId = RequestContextResolver.requireHeader(exchange, RequestHeaders.TENANT_ID);
    String organization =
        RequestContextResolver.requireHeader(exchange, RequestHeaders.ORGANIZATION_ID);
    String userId = exchange.getRequest().getHeaders().getFirst(RequestHeaders.USER_ID);
    String correlationId = exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
    return new RequestContext(
        tenantId,
        caseId,
        organization,
        StringUtils.hasText(userId) ? userId : UNKNOWN_USER,
        correlationId);
  }
}
