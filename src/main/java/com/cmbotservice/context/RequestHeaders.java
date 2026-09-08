package com.cmbotservice.context;

/**
 * HTTP header names used for request identity/tracing. Centralized so the eventual
 * switch to real authentication touches one place.
 */
public final class RequestHeaders {

    public static final String CORRELATION_ID = "X-Correlation-Id";

    /**
     * Placeholder for the authenticated analyst's identity. A POC-only stand-in for a
     * real authentication principal (JWT/session) — see {@link RequestContextResolver}.
     */
    public static final String USER_ID = "X-User-Id";

    private RequestHeaders() {
    }
}
