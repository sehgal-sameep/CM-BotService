package com.cmbotservice.common;

/**
 * Raised when the bulkhead protecting the ML Agent has no free permits (too many concurrent ML
 * Agent calls already in flight). Not an {@link MlAgentException} — the ML Agent was never
 * contacted; this is our own concurrency guard rejecting the call.
 */
public class ConcurrencyLimitExceededException extends RuntimeException {

  public ConcurrencyLimitExceededException(String message) {
    super(message);
  }
}
