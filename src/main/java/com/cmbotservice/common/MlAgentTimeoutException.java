package com.cmbotservice.common;

/**
 * The ML Agent did not respond in time — covers the first-response, idle-stream, and total-request
 * timeout concepts, all of which end up here (see {@code ChatOrchestrationService}). Not retried by
 * default: a slow/timing-out agent is the circuit breaker's problem, not the retry policy's.
 */
public class MlAgentTimeoutException extends MlAgentException {

  public MlAgentTimeoutException(String message) {
    super(message);
  }

  public MlAgentTimeoutException(String message, Throwable cause) {
    super(message, cause);
  }
}
