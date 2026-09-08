package com.cmbotservice.config;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;

/**
 * Everything needed to reach the ML Agent, whichever {@code MlAgentClient}
 * implementation is active. Validated at startup — a missing/invalid mandatory value
 * fails application boot rather than surfacing as a confusing failure on the first
 * request.
 * <p>
 * {@code baseUrl}/{@code chatPath} are only meaningful when {@code mode: http}
 * ({@link com.cmbotservice.mlagent.HttpMlAgentClient} active); they still require a
 * placeholder value in {@code mode: mock} since this whole properties object is bound
 * and validated regardless of which client bean ends up active.
 */
@ConfigurationProperties(prefix = "ml-agent")
@Validated
public record MlAgentProperties(

        @Pattern(regexp = "mock|http", message = "must be 'mock' or 'http'")
        String mode,

        @NotBlank
        String baseUrl,

        @NotBlank
        String chatPath,

        @NotNull
        Duration connectTimeout,

        @NotNull
        Duration responseTimeout,

        @NotNull
        Duration firstResponseTimeout,

        @NotNull
        Duration idleTimeout,

        @Positive
        int maxConnections,

        @NotNull
        Duration pendingAcquireTimeout,

        @NotNull
        Duration maxIdleTime,

        @NotNull
        Duration maxLifeTime,

        @NotNull
        DataSize maxInMemorySize,

        /**
         * Whether to ask the ML Agent to include a {@code suggestedResolution} in its
         * {@code payload} event. The real contract frames this as an eval-mode toggle,
         * not a per-message frontend choice, so it's a server-side default here —
         * {@code true} since that content is clearly valuable for the case manager UI.
         */
        boolean includeResolutions
) {

    public boolean isHttpMode() {
        return "http".equals(mode);
    }
}
