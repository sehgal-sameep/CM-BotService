package com.cmbotservice.common;

/**
 * Base type for every failure originating from ML Agent communication. Deliberately
 * left open (not {@code sealed}) — an {@code HttpMlAgentClient} or any future
 * {@code MlAgentClient} implementation may need to introduce further subtypes without
 * changing this class.
 */
public abstract class MlAgentException extends RuntimeException {

    protected MlAgentException(String message) {
        super(message);
    }

    protected MlAgentException(String message, Throwable cause) {
        super(message, cause);
    }
}
