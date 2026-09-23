package com.cmbotservice.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;

/**
 * The read-only string template used to look up BFF sessions ({@link JsonBlobSessionStore} or any
 * other {@link SessionStore} strategy). The {@link ReactiveRedisConnectionFactory} itself is
 * Boot-autoconfigured from {@code spring.data.redis.*} (host/port/ssl), with {@code
 * spring-cloud-azure-starter-data-redis-lettuce} transparently swapping in Entra
 * ID/managed-identity token auth whenever {@code spring.data.redis.azure.passwordless-enabled:
 * true} — this class never touches credentials directly, same as FMC-PM-BFF's own wiring. Only
 * active in {@code chatbot.security.mode: BFF_SESSION}; {@code mode: NONE} never looks anything up.
 */
@Configuration
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class RedisSessionConfig {

  /**
   * Spring Boot 4's default Jackson autoconfiguration targets Jackson 3 ({@code tools.jackson.*})
   * and does not provide a classic {@code com.fasterxml.jackson.databind.ObjectMapper} bean out of
   * the box — this app's own JSON-handling code ({@link JsonBlobSessionStore}, {@link
   * SessionAuthenticationWebFilter}) is written against the classic Jackson 2 API, so a dedicated
   * bean is provided explicitly here rather than depending on whatever the ambient framework
   * default happens to be. Configured to match Spring Boot's own long-standing default (ISO-8601
   * instants, not epoch timestamps) so a manually-serialized {@code ErrorResponse} body (this
   * class's 401/403/503 rejections) is indistinguishable in shape from one Spring's own {@code
   * GlobalExceptionHandler} path serializes for a 400.
   */
  @Bean
  public ObjectMapper securityObjectMapper() {
    return new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
  }

  @Bean
  public ReactiveStringRedisTemplate sessionRedisTemplate(
      ReactiveRedisConnectionFactory reactiveRedisConnectionFactory) {
    return new ReactiveStringRedisTemplate(reactiveRedisConnectionFactory);
  }
}
