package com.cmbotservice.common;

/**
 * Guardrails for values this service must never write to a log in the clear.
 *
 * <p>Analyst prompts and ML Agent responses are case-related free text and may contain personal
 * data — log their <i>length</i> or counts, never their content. Session identifiers and tokens are
 * credentials — log them only through {@link #maskSecret}.
 */
public final class LogSanitizer {

  private static final int VISIBLE_SUFFIX = 4;

  private LogSanitizer() {}

  /**
   * Masks a credential-like value down to its last four characters plus its length, e.g. {@code
   * ****9f3a(len=36)} — enough to tell two sessions apart or spot a truncated copy/paste, never
   * enough to reuse it.
   */
  public static String maskSecret(String secret) {
    if (secret == null) {
      return "<absent>";
    }
    if (secret.isEmpty()) {
      return "<empty>";
    }
    if (secret.length() <= VISIBLE_SUFFIX * 2) {
      return "****(len=" + secret.length() + ")";
    }
    return "****"
        + secret.substring(secret.length() - VISIBLE_SUFFIX)
        + "(len="
        + secret.length()
        + ")";
  }

  /**
   * One line naming every exception in the cause chain, outermost first — e.g. {@code
   * RedisConnectionFailureException: Unable to connect to Redis -> RedisConnectionException: Unable
   * to connect to localhost/127.0.0.1:6379 -> ConnectException: Connection refused}. The innermost
   * entry is usually the real reason (refused, timed out, DNS, TLS handshake, auth).
   */
  public static String causeChain(Throwable ex) {
    StringBuilder chain = new StringBuilder();
    Throwable current = ex;
    int depth = 0;
    while (current != null && depth++ < 8) {
      if (!chain.isEmpty()) {
        chain.append(" -> ");
      }
      chain.append(current.getClass().getSimpleName()).append(": ").append(current.getMessage());
      if (current.getCause() == current) {
        break;
      }
      current = current.getCause();
    }
    return chain.toString();
  }
}
