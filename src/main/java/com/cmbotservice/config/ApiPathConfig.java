package com.cmbotservice.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.method.HandlerTypePredicate;
import org.springframework.web.reactive.config.PathMatchConfigurer;
import org.springframework.web.reactive.config.WebFluxConfigurer;

/**
 * Prefixes every controller in {@code com.cmbotservice.web.controller} with {@code
 * chatbot.api.base-path}. Scoped by package rather than {@code @RestController} so springdoc's own
 * controllers (Swagger UI, {@code /v3/api-docs}) and actuator stay at their usual paths.
 */
@Configuration
public class ApiPathConfig implements WebFluxConfigurer {

  private static final Logger log = LoggerFactory.getLogger(ApiPathConfig.class);

  private static final String CONTROLLER_PACKAGE = "com.cmbotservice.web.controller";

  private final ApiProperties apiProperties;

  public ApiPathConfig(ApiProperties apiProperties) {
    this.apiProperties = apiProperties;
  }

  @Override
  public void configurePathMatching(PathMatchConfigurer configurer) {
    if (apiProperties.basePath().isEmpty()) {
      log.info("API_BASE_PATH_CONFIGURED basePath=<none>");
      return;
    }
    configurer.addPathPrefix(
        apiProperties.basePath(), HandlerTypePredicate.forBasePackage(CONTROLLER_PACKAGE));
    log.info("API_BASE_PATH_CONFIGURED basePath={}", apiProperties.basePath());
  }
}
