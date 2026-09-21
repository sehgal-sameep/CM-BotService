package com.cmbotservice.context;

import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

/**
 * Resolves "who is calling, for which tenant/organization/case" into a {@link RequestContext}.
 * {@code tenantId}/{@code organization} are read from the {@code X-Tenant-Id}/{@code X-Org-Id}
 * request headers (see {@link RequestHeaders}) by every implementation — both are required, and an
 * implementation rejects the request (400, {@code ErrorCode.VALIDATION_ERROR}) if either is missing
 * or blank.
 *
 * <p>This was the single seam real authentication plugged in at: {@link
 * HeaderBasedRequestContextResolver} (active in {@code chatbot.security.mode: NONE}, trusting an
 * {@code X-User-Id} header) and {@code com.cmbotservice.security.SessionRequestContextResolver}
 * (active in {@code mode: BFF_SESSION}, backed by a validated session) are selected purely by that
 * one property — no controller or service code change either way, since they only ever consume the
 * resolved {@link RequestContext}.
 */
public interface RequestContextResolver {

  RequestContext resolve(ServerWebExchange exchange, String caseId);

  /**
   * Shared by every implementation to enforce the required-header rule documented above, so the
   * same 400/{@code ResponseStatusException} behavior for a missing/blank header doesn't have to be
   * duplicated per implementation.
   */
  static String requireHeader(ServerWebExchange exchange, String headerName) {
    String value = exchange.getRequest().getHeaders().getFirst(headerName);
    if (!StringUtils.hasText(value)) {
      throw new ResponseStatusException(
          HttpStatus.BAD_REQUEST, headerName + " header must not be blank");
    }
    return value;
  }
}
