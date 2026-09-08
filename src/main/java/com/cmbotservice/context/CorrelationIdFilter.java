package com.cmbotservice.context;

import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.util.UUID;

/**
 * Stamps every request with a correlation ID (accepted from {@code X-Correlation-Id} or
 * generated), echoes it on the response, and writes it into the Reactor {@link
 * reactor.util.context.Context} for the duration of the request — so it can be
 * restored into SLF4J MDC on whatever thread ends up logging, however many operator/
 * thread hops later (see {@link MdcContext}), without leaking into any other request's
 * context.
 * <p>
 * {@code tenantId}/{@code caseId}/{@code conversationId} live in the request body
 * (this service has no per-request URL path segments to parse them from), so they're
 * added to the same {@code Context} later, once the body is deserialized — see
 * {@code ChatOrchestrationService}.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class CorrelationIdFilter implements WebFilter {

    public static final String CORRELATION_ID_ATTRIBUTE = "correlationId";

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
        String correlationId = exchange.getRequest().getHeaders().getFirst(RequestHeaders.CORRELATION_ID);
        if (!StringUtils.hasText(correlationId)) {
            correlationId = UUID.randomUUID().toString();
        }
        exchange.getAttributes().put(CORRELATION_ID_ATTRIBUTE, correlationId);
        exchange.getResponse().getHeaders().add(RequestHeaders.CORRELATION_ID, correlationId);

        String finalCorrelationId = correlationId;
        return chain.filter(exchange)
                .contextWrite(ctx -> ctx.put(MdcContext.CORRELATION_ID, finalCorrelationId));
    }
}
