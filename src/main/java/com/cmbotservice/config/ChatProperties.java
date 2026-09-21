package com.cmbotservice.config;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Chat-request-level limits. {@code maxMessageLength} is an operationally tunable ceiling checked
 * at runtime in {@code ChatOrchestrationService}; it works alongside (not instead of) the hard
 * compile-time {@code @Size(max = 4000)} backstop on {@code ChatRequest.message} — the annotation
 * is the absolute limit this API will ever accept, this property lets that limit be tightened
 * further without a redeploy.
 */
@ConfigurationProperties(prefix = "chat")
@Validated
public record ChatProperties(@Positive int maxMessageLength, @NotNull Duration maxStreamDuration) {}
