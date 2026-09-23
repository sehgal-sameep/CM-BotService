package com.cmbotservice.common;

/**
 * The ML Agent responded, but an event could not be relayed at all — i.e. it could not be rendered
 * into its canonical JSON form for the SSE stream. Never retryable: the agent already answered, so
 * retrying would just get the same data again. (This backend does not otherwise judge the content
 * of the agent's events; it forwards them as-is.)
 */
public class MlAgentMalformedResponseException extends MlAgentException {

  public MlAgentMalformedResponseException(String message) {
    super(message);
  }

  public MlAgentMalformedResponseException(String message, Throwable cause) {
    super(message, cause);
  }
}
