package com.cmbotservice.security;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Everything needed to enforce (or bypass) the simplified BFF-session authentication flow.
 * Validated at startup — a missing/invalid mandatory value fails application boot rather than
 * surfacing as a confusing failure on the first request, same principle as {@link
 * com.cmbotservice.config.MlAgentProperties}.
 *
 * <p>{@code redis}/{@code cors} are only meaningful when {@code mode: BFF_SESSION}; they still
 * require valid values in {@code mode: NONE} since this whole properties object is bound and
 * validated regardless of which mode ends up active.
 *
 * <p>The actual Redis connection ({@code spring.data.redis.*}, including Azure Entra ID
 * passwordless auth) is Boot/Azure-starter-managed, not part of this properties object — see {@link
 * RedisSessionConfig}. {@code redis.fieldNames} here only names the fields of the confirmed
 * FMC-PM-BFF session envelope (see {@link JsonBlobSessionStore}), kept configurable so a naming
 * change needs a config change, not a code change.
 *
 * <p><b>Current scope is deliberately minimal</b>: a found Redis session is trusted as
 * authenticated outright — no fingerprint, CSRF, permission, or token-expiry validation. {@code
 * csrf}/{@code authorization} config, once present for that richer (diagrammed) flow, has been
 * removed rather than left unused; reintroduce alongside the corresponding filter logic if/when
 * that scope comes back.
 */
@ConfigurationProperties(prefix = "chatbot.security")
@Validated
public record SecurityProperties(
    @NotNull ChatbotSecurityMode mode,
    @NotNull @Valid Session session,
    @NotNull @Valid Redis redis,
    @NotNull @Valid Cors cors,

    /**
     * Insecure escape hatch for local development only: if the shared Redis is unreachable, permit
     * the request through anyway (fail open) instead of rejecting it. Must never be {@code true} in
     * a shared/prod environment — the documented default behavior is to reject with 503 when Redis
     * is down.
     */
    boolean failOpenOnRedisError) {

  public boolean isBffSessionMode() {
    return mode == ChatbotSecurityMode.BFF_SESSION;
  }

  public record Session(
      @NotBlank String cookieName,
      /**
       * Header carrying the caller's tenant for the BFF-session lookup — part of the Redis key
       * ({@code session:<sessionId>:<tenant>}). Defaults to the same always-required {@code
       * X-Tenant-Id} header ({@link com.cmbotservice.context.RequestHeaders#TENANT_ID}) every
       * request already sends regardless of security mode, rather than a separate custom header —
       * kept as its own property only so a future divergence needs a config change, not a code
       * change.
       */
      @NotBlank String tenantHeaderName) {}

  public record Redis(
      @NotBlank String strategy,
      @NotBlank String namespace,
      @NotNull @Valid FieldNames fieldNames) {

    /**
     * Field names of the session envelope stored per session — see {@link JsonBlobSessionStore}'s
     * class-level Javadoc for the confirmed key/value shape.
     */
    public record FieldNames(
        @NotBlank String contextJson,
        @NotBlank String accessToken,
        @NotBlank String refreshToken,
        @NotBlank String fingerprint) {}
  }

  public record Cors(List<String> allowedOrigins) {}
}
