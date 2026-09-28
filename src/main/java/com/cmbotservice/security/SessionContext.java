package com.cmbotservice.security;

/**
 * The session record read back from Redis, deliberately transport/storage-agnostic so {@link
 * SessionAuthenticationWebFilter} and everything downstream of it never touch a raw Redis value
 * directly. {@code username}/{@code tenantId} come from parsing the nested {@code context_json}
 * string; {@code accessToken}/{@code refreshToken}/{@code fingerprint} are carried through
 * unchanged from the envelope.
 *
 * <p><b>Current scope</b>: finding a record at all is treated as "authenticated" — {@code
 * fingerprint} is carried through only for {@link
 * com.cmbotservice.web.controller.SessionDebugController} to expose (no comparison is performed
 * against it), and {@code accessToken} is forwarded to the ML Agent as {@code authorization:
 * Bearer} gRPC call metadata.
 */
public record SessionContext(
    String username,
    String tenantId,
    String accessToken,
    String refreshToken,
    String contextJson,
    String fingerprint) {

  /** Redacts every credential field so a session can't leak tokens through a log line. */
  @Override
  public String toString() {
    return "SessionContext[username=%s, tenantId=%s, accessTokenPresent=%s, refreshTokenPresent=%s]"
        .formatted(
            username,
            tenantId,
            accessToken != null && !accessToken.isBlank(),
            refreshToken != null && !refreshToken.isBlank());
  }

  /**
   * The exchange attribute key {@link SessionAuthenticationWebFilter} stores this under, and {@link
   * SessionRequestContextResolver} reads it back from.
   */
  public static final String EXCHANGE_ATTRIBUTE = SessionContext.class.getName();
}
