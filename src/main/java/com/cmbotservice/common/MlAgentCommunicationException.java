package com.cmbotservice.common;

/**
 * The ML Agent was reached and responded, but the response itself indicates a
 * transient server-side failure (e.g. a 5xx status) — or, for {@code MockMlAgentClient},
 * a simulated failure (`trigger:error`). Retryable, subject to the same
 * before-first-event rule as every other retryable {@link MlAgentException}.
 */
public class MlAgentCommunicationException extends MlAgentException {

    public MlAgentCommunicationException(String message) {
        super(message);
    }

    public MlAgentCommunicationException(String message, Throwable cause) {
        super(message, cause);
    }
}
