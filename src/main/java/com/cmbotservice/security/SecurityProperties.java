package com.cmbotservice.security;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

/**
 * Everything needed to enforce (or bypass) the BFF-session authentication flow. Validated at
 * startup — a missing/invalid mandatory value fails application boot rather than surfacing as a
 * confusing failure on the first request, same principle as {@link
 * com.cmbotservice.config.MlAgentProperties}.
 *
 * <p>{@code redis}/{@code csrf}/{@code authorization}/{@code cors} are only meaningful when {@code
 * mode: BFF_SESSION}; they still require valid values in {@code mode: NONE} since this whole
 * properties object is bound and validated regardless of which mode ends up active.
 *
 * <p><b>{@code redis.fieldNames} maps a genuinely unconfirmed dependency</b>: the exact Redis
 * session layout FMC-PM-BFF uses hasn't been confirmed (see {@link JsonBlobSessionStore}). Every
 * field name is configurable precisely so a layout correction needs a config change, not a code
 * change, in the common case.
 */
@ConfigurationProperties(prefix = "chatbot.security")
@Validated
public record SecurityProperties(
    @NotNull ChatbotSecurityMode mode,
    @NotNull @Valid Session session,
    @NotNull @Valid Redis redis,
    @NotNull @Valid Csrf csrf,
    @NotNull @Valid Authorization authorization,
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

  public record Session(@NotBlank String cookieName, @NotBlank String tenantHeaderName) {}

  public record Redis(
      @NotBlank String strategy,
      @NotBlank String namespace,
      @NotBlank String host,
      @Positive int port,
      boolean ssl,
      String password,
      @NotNull @Valid FieldNames fieldNames) {

    /**
     * Field names inside the (assumed) single JSON document stored per session — see {@link
     * JsonBlobSessionStore}'s class-level Javadoc for the full caveat.
     */
    public record FieldNames(
        @NotBlank String username,
        @NotBlank String tenantId,
        @NotBlank String permissions,
        @NotBlank String organizations,
        @NotBlank String accessTokenExpiry,
        @NotBlank String fingerprint) {}
  }

  public record Csrf(boolean enabled, @NotBlank String cookieName, @NotBlank String headerName) {}

  public record Authorization(
      @NotBlank String requiredPermissionPrefix, boolean permitWhenNoChatbotPermissions) {}

  public record Cors(List<String> allowedOrigins) {}
}
