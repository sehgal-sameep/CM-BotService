package com.cmbotservice.config;

import io.github.resilience4j.bulkhead.Bulkhead;
import io.github.resilience4j.bulkhead.BulkheadConfig;
import io.github.resilience4j.bulkhead.BulkheadRegistry;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds the single named ("mlAgent") circuit breaker and bulkhead instances that guard every ML
 * Agent call, from our own {@link ResilienceProperties} namespace rather than resilience4j's native
 * {@code resilience4j.circuitbreaker.instances.*} YAML — matching the configuration shape this
 * project's brief specifies — while still using the auto-configured (otherwise-empty) registries
 * from {@code resilience4j-spring-boot4}, which is what wires up {@code /actuator/circuitbreakers},
 * the circuit-breaker health indicator, and Micrometer metrics ({@code
 * resilience4j.circuitbreaker.*}/{@code resilience4j.bulkhead.*}) for free — the starter's own
 * autoconfiguration binds metrics for every instance it finds in these registries as they're
 * created, so no metrics-binding code is needed here.
 */
@Configuration
public class ResilienceConfig {

  public static final String ML_AGENT = "mlAgent";

  @Bean
  public CircuitBreaker mlAgentCircuitBreaker(
      CircuitBreakerRegistry registry, ResilienceProperties properties) {
    ResilienceProperties.CircuitBreaker cfg = properties.circuitBreaker();
    CircuitBreakerConfig circuitBreakerConfig =
        CircuitBreakerConfig.custom()
            .failureRateThreshold(cfg.failureRateThreshold())
            .slidingWindowSize(cfg.slidingWindowSize())
            .waitDurationInOpenState(cfg.waitDurationInOpenState())
            .permittedNumberOfCallsInHalfOpenState(cfg.permittedCallsInHalfOpenState())
            .minimumNumberOfCalls(cfg.minimumNumberOfCalls())
            .build();
    return registry.circuitBreaker(ML_AGENT, circuitBreakerConfig);
  }

  @Bean
  public Bulkhead mlAgentBulkhead(BulkheadRegistry registry, ResilienceProperties properties) {
    ResilienceProperties.Bulkhead cfg = properties.bulkhead();
    BulkheadConfig bulkheadConfig =
        BulkheadConfig.custom()
            .maxConcurrentCalls(cfg.maxConcurrentCalls())
            .maxWaitDuration(cfg.maxWaitDuration())
            .build();
    return registry.bulkhead(ML_AGENT, bulkheadConfig);
  }
}
