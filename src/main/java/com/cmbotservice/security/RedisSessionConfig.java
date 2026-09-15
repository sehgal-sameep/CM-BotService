package com.cmbotservice.security;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.util.StringUtils;

/**
 * The single shared, read-only Redis connection used to look up BFF sessions
 * ({@link JsonBlobSessionStore} or any other {@link SessionStore} strategy) — built
 * once at startup, never per-request, same principle as this service's other pooled
 * clients ({@code GrpcChannelConfig}). Only created in {@code chatbot.security.mode:
 * BFF_SESSION}; {@code mode: NONE} never opens a Redis connection at all.
 */
@Configuration
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class RedisSessionConfig {

    /**
     * Spring Boot 4's default Jackson autoconfiguration targets Jackson 3
     * ({@code tools.jackson.*}) and does not provide a classic
     * {@code com.fasterxml.jackson.databind.ObjectMapper} bean out of the box — this
     * app's own JSON-handling code ({@link JsonBlobSessionStore},
     * {@link SessionAuthenticationWebFilter}) is written against the classic Jackson 2
     * API, so a dedicated bean is provided explicitly here rather than depending on
     * whatever the ambient framework default happens to be. Configured to match
     * Spring Boot's own long-standing default (ISO-8601 instants, not epoch
     * timestamps) so a manually-serialized {@code ErrorResponse} body (this class's
     * 401/403/503 rejections) is indistinguishable in shape from one Spring's own
     * {@code GlobalExceptionHandler} path serializes for a 400.
     */
    @Bean
    public ObjectMapper securityObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    @Bean
    public ReactiveRedisConnectionFactory sessionRedisConnectionFactory(SecurityProperties properties) {
        SecurityProperties.Redis redis = properties.redis();

        RedisStandaloneConfiguration standaloneConfig = new RedisStandaloneConfiguration(redis.host(), redis.port());
        if (StringUtils.hasText(redis.password())) {
            standaloneConfig.setPassword(redis.password());
        }

        LettuceClientConfiguration.LettuceClientConfigurationBuilder clientConfigBuilder = LettuceClientConfiguration.builder();
        if (redis.ssl()) {
            clientConfigBuilder.useSsl();
        }

        return new LettuceConnectionFactory(standaloneConfig, clientConfigBuilder.build());
    }

    @Bean
    public ReactiveStringRedisTemplate sessionRedisTemplate(ReactiveRedisConnectionFactory sessionRedisConnectionFactory) {
        return new ReactiveStringRedisTemplate(sessionRedisConnectionFactory);
    }
}
