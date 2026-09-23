package com.cmbotservice.web.dto;

import io.swagger.v3.oas.annotations.media.Schema;

/**
 * The raw session record {@code SessionDebugController} read back from Redis — every field is
 * exactly what's stored, unvalidated, for manual verification that the lookup itself works.
 *
 * <p>There is no expiry/TTL field: this service doesn't decode {@code access_token}'s claims, and
 * Redis's own key TTL isn't queryable through this endpoint.
 */
@Schema(
    description =
        "Raw BFF session record retrieved from Redis, for TEMPORARY debug/verification use only. "
            + "Every field is exactly what's stored at the session key, with no validation applied.")
public record SessionDebugResponse(
    @Schema(description = "Parsed out of the session's context_json field.", example = "alice")
        String username,
    @Schema(description = "Parsed out of the session's context_json field.", example = "tenant-1")
        String tenantId,
    @Schema(description = "Raw access_token value stored in the session record (a JWT).")
        String accessToken,
    @Schema(description = "Raw refresh_token value stored in the session record, if present.")
        String refreshToken,
    @Schema(
            description =
                "The raw context_json string stored in the session record, exactly as retrieved"
                    + " (not re-serialized) — includes fields beyond username/tenantId that this"
                    + " service otherwise ignores.")
        String contextJson,
    @Schema(
            description =
                "Raw fp (client fingerprint) value stored in the session record, if present.")
        String fingerprint) {}
