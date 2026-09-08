package com.cmbotservice.common;

/**
 * The ML Agent could not be reached at all — connection refused/reset, DNS failure,
 * or an equivalent connection-establishment failure. Retryable: this is precisely the
 * "connection establishment fails" / "connection reset before response begins" case
 * the retry policy exists for.
 */
public class MlAgentUnavailableException extends MlAgentException {

    public MlAgentUnavailableException(String message) {
        super(message);
    }

    public MlAgentUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
