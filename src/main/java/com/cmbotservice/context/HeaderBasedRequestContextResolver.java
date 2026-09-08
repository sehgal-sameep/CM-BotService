package com.cmbotservice.context;

import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;

/**
 * POC {@link RequestContextResolver}: trusts an {@code X-User-Id} header for analyst
 * identity. Not a real authentication mechanism — intended to be replaced by a
 * principal-based resolver (JWT/session) once the platform's auth story is decided,
 * with no change required to any consumer of {@link RequestContext}.
 */
@Component
public class HeaderBasedRequestContextResolver implements RequestContextResolver {

    private static final String UNKNOWN_USER = "unknown-user";

    @Override
    public RequestContext resolve(ServerWebExchange exchange, String tenantId, String caseId) {
        String userId = exchange.getRequest().getHeaders().getFirst(RequestHeaders.USER_ID);
        String correlationId = exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
        return new RequestContext(
                tenantId,
                caseId,
                StringUtils.hasText(userId) ? userId : UNKNOWN_USER,
                correlationId
        );
    }
}
