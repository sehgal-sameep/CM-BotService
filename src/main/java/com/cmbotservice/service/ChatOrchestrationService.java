package com.cmbotservice.service;

import com.cmbotservice.context.RequestContext;
import com.cmbotservice.web.dto.ChatRequest;
import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

/**
 * Glue between the web layer and the ML Agent: forwards one chat message, applies
 * resilience (circuit breaker, bulkhead, timeout, retry), and relays the response as a
 * stream of SSE events. This is the one contract {@link com.cmbotservice.web.controller.ChatController}
 * depends on — it never sees {@link ChatOrchestrationServiceImpl} or any resilience/ML
 * Agent detail directly, so that implementation is free to change (e.g. a different
 * resilience strategy, or splitting the pipeline differently) without touching the web
 * layer.
 */
public interface ChatOrchestrationService {

    /**
     * Streams the ML Agent's response to one chat message as a cold {@code Flux}:
     * nothing happens until subscribed, and every failure — before or after the
     * response has started streaming — surfaces as an {@code error} SSE event on this
     * same stream rather than a distinct HTTP status, since the response is already
     * committed at 200 by the time Spring starts writing elements.
     */
    Flux<ServerSentEvent<Object>> streamMessage(RequestContext context, ChatRequest request);
}
