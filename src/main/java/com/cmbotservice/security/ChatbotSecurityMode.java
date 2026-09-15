package com.cmbotservice.security;

/**
 * Master toggle for the whole authentication flow, bound from
 * {@code chatbot.security.mode}.
 */
public enum ChatbotSecurityMode {

    /**
     * Full validation flow enabled: session lookup, CSRF, tenant, and permission
     * checks are all enforced. The only mode intended for any shared/production
     * environment.
     */
    BFF_SESSION,

    /**
     * Bypasses all authentication and permits every request unchanged — local
     * development only. {@link SecurityModeStartupLogger} logs a loud warning on
     * every startup while this is active.
     */
    NONE
}
