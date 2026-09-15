package com.cmbotservice.security;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

import java.util.List;

/**
 * Restricts cross-origin requests to the explicit FMC UI origins configured under
 * {@code chatbot.security.cors.allowed-origins}, with credentials (the session
 * cookie) allowed — never a wildcard origin, per the documented flow. Uses Spring's
 * own {@link CorsWebFilter}/{@link CorsConfiguration} rather than hand-rolled origin
 * matching. Only active in {@code chatbot.security.mode: BFF_SESSION} — CORS
 * restriction is part of "all other behavior" the master toggle gates, same as every
 * other check in this flow.
 */
@Configuration
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class CorsSecurityConfig {

    @Bean
    public CorsWebFilter corsWebFilter(SecurityProperties properties) {
        CorsConfiguration configuration = new CorsConfiguration();
        configuration.setAllowedOrigins(properties.cors().allowedOrigins());
        configuration.setAllowCredentials(true);
        configuration.setAllowedMethods(List.of("GET", "POST"));
        configuration.setAllowedHeaders(List.of("*"));

        UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
        source.registerCorsConfiguration("/**", configuration);
        return new CorsWebFilter(source);
    }
}
