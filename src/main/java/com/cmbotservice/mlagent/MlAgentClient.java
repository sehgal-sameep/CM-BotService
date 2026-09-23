package com.cmbotservice.mlagent;

import com.cmbotservice.mlagent.grpc.v1.AnswerEvent;
import reactor.core.publisher.Flux;

/**
 * Integration boundary to the external ML/AI Agent, owned by a separate team.
 *
 * <p>This is the one seam the whole application depends on for ML functionality — controllers and
 * orchestration logic depend only on this interface (Dependency Inversion). {@link
 * MockMlAgentClient} and {@code GrpcMlAgentClient} are the two implementations, selected via the
 * {@code ml-agent.mode} property ({@code @ConditionalOnProperty} on each — see either class), never
 * a runtime {@code if/else} in orchestration code.
 *
 * <p>Implementations emit the ML Agent's own {@link AnswerEvent} messages exactly as received —
 * every event type (including {@code error} and {@code ping}) and every field, untranslated. This
 * backend is a transparent proxy for the response contract: {@code ChatOrchestrationService}
 * forwards each event to the frontend verbatim (see {@code com.cmbotservice.sse.SseEvents}), so
 * there is deliberately no backend-owned domain model of the response to map into.
 */
public interface MlAgentClient {

  /**
   * Streams the ML Agent's response for {@code request} as a cold, backpressure-respecting {@code
   * Flux}: nothing happens until subscribed, and cancelling the subscription (a client disconnect
   * propagates here automatically) stops any further work. Transport-level failures (the agent
   * could not be reached, rejected the call, etc.) surface on the {@code Flux}'s error channel as
   * {@code MlAgentException} subtypes; model-level failures are the agent's own {@code error}
   * event, emitted like any other event. Timeout, retry, circuit-breaker, and bulkhead behavior are
   * applied uniformly to whatever this returns by {@code ChatOrchestrationService} —
   * implementations should not attempt any of that themselves.
   */
  Flux<AnswerEvent> streamResponse(MlAgentRequest request);
}
