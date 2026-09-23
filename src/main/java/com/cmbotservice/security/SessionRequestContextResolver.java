package com.cmbotservice.security;

import com.cmbotservice.context.CorrelationIdFilter;
import com.cmbotservice.context.RequestContext;
import com.cmbotservice.context.RequestContextResolver;
import com.cmbotservice.context.RequestHeaders;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;

/**
 * {@code chatbot.security.mode: BFF_SESSION} {@link RequestContextResolver}: derives {@code
 * userId}/{@code accessToken} from the {@link SessionContext} that {@link
 * SessionAuthenticationWebFilter} already found and stored as an exchange attribute — this is
 * exactly the seam {@link RequestContextResolver}'s own Javadoc anticipated, so {@code
 * ChatController} requires no change at all to pick this up.
 *
 * <p>{@code tenantId}/{@code organization} are read from the {@code X-Tenant-Id}/ {@code X-Org-Id}
 * request headers, same as {@link com.cmbotservice.context.HeaderBasedRequestContextResolver} —
 * {@code caseId} is still sourced from the request body, unchanged from today's contract.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class SessionRequestContextResolver implements RequestContextResolver {

  private static final String UNKNOWN_USER = "unknown-user";

  @Override
  public RequestContext resolve(ServerWebExchange exchange, String caseId) {
    SessionContext session = exchange.getAttribute(SessionContext.EXCHANGE_ATTRIBUTE);
    String correlationId = exchange.getAttribute(CorrelationIdFilter.CORRELATION_ID_ATTRIBUTE);
    // session is only ever absent here if chatbot.security.fail-open-on-redis-error
    // let a request through without one (see SessionAuthenticationWebFilter) —
    // every other path through the filter either populates it or rejects the
    // request outright before this resolver ever runs.
    String userId = session != null ? session.username() : UNKNOWN_USER;
    String accessToken = session != null ? session.accessToken() : null;
    String tenantId = RequestContextResolver.requireHeader(exchange, RequestHeaders.TENANT_ID);
    String organization =
        RequestContextResolver.requireHeader(exchange, RequestHeaders.ORGANIZATION_ID);
    return new RequestContext(tenantId, caseId, organization, userId, correlationId, accessToken);
  }
}
