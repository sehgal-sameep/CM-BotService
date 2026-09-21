package com.cmbotservice.security;

import java.time.Instant;
import java.util.List;

/**
 * The "common internal context" the authentication flow parses a Redis session record into —
 * deliberately transport/storage-agnostic, so {@link SessionAuthenticationWebFilter} and everything
 * downstream of it never touch a raw Redis value directly.
 *
 * <p>{@code permissions} here is the session's <b>full, unfiltered</b> permission list as read from
 * the store; {@link SessionAuthenticationWebFilter} separately computes the {@code
 * CHATBOT_}-prefixed subset ("granted authorities") and rejects the request if that subset is empty
 * — {@code permissions} is kept whole on this record so a future endpoint-specific permission check
 * (not implemented today — see README/ARCHITECTURE "known limitations") has the full list to work
 * from.
 */
public record SessionContext(
    String username,
    String tenantId,
    List<String> permissions,
    List<String> organizations,
    Instant accessTokenExpiry,
    String fingerprint) {

  /**
   * The exchange attribute key {@link SessionAuthenticationWebFilter} stores this under, and {@link
   * SessionRequestContextResolver} reads it back from.
   */
  public static final String EXCHANGE_ATTRIBUTE = SessionContext.class.getName();

  /**
   * Fail-closed on purpose: a session record with no expiry at all is treated as already expired,
   * not as never-expiring. {@code accessTokenExpiry} is one of the fields the flow documents as
   * always present (only {@code fingerprint} is documented as optional), so a missing value here
   * indicates a malformed record, not a legitimately expiry-free session.
   */
  public boolean isExpired() {
    return accessTokenExpiry == null || Instant.now().isAfter(accessTokenExpiry);
  }
}
