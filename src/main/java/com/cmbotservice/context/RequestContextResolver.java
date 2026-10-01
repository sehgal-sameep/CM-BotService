package com.cmbotservice.context;

import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;

/**
 * Resolves "who is calling, for which tenant/organization/case" into a {@link RequestContext}.
 * {@code tenantId}/{@code organization} are read from the {@code X-Tenant-Id}/{@code X-Org-Id}
 * request headers (see {@link RequestHeaders}) by every implementation. {@code X-Tenant-Id} is
 * required — an implementation rejects the request (400, {@code ErrorCode.VALIDATION_ERROR}) if it
 * is missing or blank. {@code X-Org-Id} is optional — missing or blank resolves to a {@code null}
 * organization, which is then not forwarded to the ML Agent.
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

  /**
   * Shared by every implementation for an optional header: its value, or {@code null} if it is
   * missing or blank — never a default, so an absent header is never forwarded as something else.
   */
  static String optionalHeader(ServerWebExchange exchange, String headerName) {
    String value = exchange.getRequest().getHeaders().getFirst(headerName);
    return StringUtils.hasText(value) ? value : null;
  }
}
