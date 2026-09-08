package com.cmbotservice.web;

/**
 * Shared path constants. There is a single stateless endpoint — {@code tenantId},
 * {@code caseId}, and {@code conversationId} travel in the request body (see
 * {@link com.cmbotservice.web.dto.ChatRequest}), not the URL, since there is no
 * backend-owned resource to nest a path under.
 */
public final class ApiPaths {

    public static final String CHAT_MESSAGES = "/api/v1/chat/messages";

    private ApiPaths() {
    }
}
