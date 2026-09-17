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
 * {@code grpcHost}/{@code grpcPort} are only meaningful when {@code mode: grpc}
 * ({@link com.cmbotservice.mlagent.GrpcMlAgentClient} active); they still require a
 * placeholder value in {@code mode: mock} since this whole properties object is bound
 * and validated regardless of which client bean ends up active.
 */
@ConfigurationProperties(prefix = "ml-agent")
@Validated
public record MlAgentProperties(

        @Pattern(regexp = "mock|grpc", message = "must be 'mock' or 'grpc'")
        String mode,

        @NotBlank
        String grpcHost,

        @Positive
        int grpcPort,

        @NotNull
        Duration firstResponseTimeout,

        @NotNull
        Duration idleTimeout,

        @NotNull
        DataSize grpcMaxInboundMessageSize
) {

    public boolean isGrpcMode() {
        return "grpc".equals(mode);
    }
}
