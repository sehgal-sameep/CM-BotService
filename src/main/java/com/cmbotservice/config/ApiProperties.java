package com.cmbotservice.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * {@code basePath} is the common prefix for every endpoint this service's own controllers expose
 * (e.g. {@code /back-office-ai} + {@code /api/v1/chat/messages}) — applied in {@link
 * ApiPathConfig}. Actuator and Swagger are deliberately not prefixed, so health probes keep their
 * fixed paths. Normalized to a leading {@code /} and no trailing {@code /}; blank means no prefix.
 */
@ConfigurationProperties(prefix = "chatbot.api")
public record ApiProperties(String basePath) {

  public ApiProperties {
    String trimmed = basePath == null ? "" : basePath.trim();
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    if (!trimmed.isEmpty() && !trimmed.startsWith("/")) {
      trimmed = "/" + trimmed;
    }
    basePath = trimmed;
  }

  /** {@code path} (one of {@link com.cmbotservice.web.ApiPaths}) as actually served. */
  public String fullPath(String path) {
    return basePath + path;
  }
}
