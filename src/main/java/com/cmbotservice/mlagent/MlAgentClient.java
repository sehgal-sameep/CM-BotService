package com.cmbotservice.mlagent;

import reactor.core.publisher.Flux;

/**
 * Integration boundary to the external ML/AI Agent, owned by a separate team.
 * <p>
 * This is the one seam the whole application depends on for ML functionality —
 * controllers and orchestration logic depend only on this interface (Dependency
 * Inversion). {@link MockMlAgentClient} and {@code GrpcMlAgentClient} are the two
 * implementations, selected via the {@code ml-agent.mode} property
 * ({@code @ConditionalOnProperty} on each — see either class), never a runtime
 * {@code if/else} in orchestration code.
 */
public interface MlAgentClient {

    /**
     * Streams a response for {@code request} as a cold, backpressure-respecting
     * {@code Flux}: nothing happens until subscribed, and cancelling the subscription
     * (a client disconnect propagates here automatically) stops any further work.
     * Timeout, retry, circuit-breaker, and bulkhead behavior are applied uniformly to
     * whatever this returns by {@code ChatOrchestrationService} — implementations
     * should not attempt any of that themselves.
     */
    Flux<MlAgentStreamEvent> streamResponse(MlAgentRequest request);
}
