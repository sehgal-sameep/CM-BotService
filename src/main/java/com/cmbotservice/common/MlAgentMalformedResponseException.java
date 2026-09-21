package com.cmbotservice.common;

/**
 * The ML Agent responded, but the response itself is invalid — an unrecognized event type, a
 * required field missing, or a non-monotonic sequence number. Never retryable: the agent already
 * answered, so retrying would just get the same malformed data again.
 */
public class MlAgentMalformedResponseException extends MlAgentException {

  public MlAgentMalformedResponseException(String message) {
    super(message);
  }
}
