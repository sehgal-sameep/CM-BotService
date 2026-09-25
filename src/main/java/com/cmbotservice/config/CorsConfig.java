package com.cmbotservice.config;

import com.cmbotservice.context.RequestHeaders;
import com.cmbotservice.security.SecurityProperties;
import java.time.Duration;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.cors.CorsConfiguration;
import org.springframework.web.cors.reactive.CorsWebFilter;
import org.springframework.web.cors.reactive.UrlBasedCorsConfigurationSource;

/**
 * CORS for every path, in every security mode. By default any origin is allowed, with credentials
 * (the BFF {@code SESSION} cookie), any method and any request header, so a browser frontend on
 * another origin never hits a CORS error. {@code chatbot.security.cors.allowed-origins} ({@code
 * CORS_ALLOWED_ORIGINS}) can narrow that to a list of origins or origin patterns (e.g. {@code
 * https://*.example.com}); unset, empty, or {@code *} means "allow all".
 *
 * <p>Browsers reject {@code Access-Control-Allow-Origin: *} on credentialed requests, so "allow
 * all" is implemented with the origin <i>pattern</i> {@code *}, which echoes the caller's own
 * {@code Origin} back — the only way to allow every origin and still send cookies.
 *
 * <p>Ordered right after {@code CorrelationIdFilter} and {@code RequestLoggingFilter} (so a
 * preflight is still correlated and logged), and ahead of {@code SessionAuthenticationWebFilter}.
 * Two reasons: the browser's {@code OPTIONS} preflight carries no cookie, so if authentication ran
 * first it would answer 401 and the browser would report a CORS error; and CORS headers must
 * already be on the response when the auth filter writes a 401/503, or the frontend sees "CORS
 * error" instead of the real status.
 */
@Configuration
@Slf4j
public class CorsConfig {

  private static final String ALLOW_ALL = "*";

  @Bean
  @Order(Ordered.HIGHEST_PRECEDENCE + 2)
  public CorsWebFilter corsWebFilter(SecurityProperties properties) {
    List<String> origins = allowedOriginPatterns(properties.cors().allowedOrigins());

    CorsConfiguration configuration = new CorsConfiguration();
    configuration.setAllowedOriginPatterns(origins);
    configuration.setAllowCredentials(true);
    configuration.setAllowedMethods(List.of(ALLOW_ALL));
    configuration.setAllowedHeaders(List.of(ALLOW_ALL));
    // Let the frontend read the correlation id to quote in bug reports / match to logs.
    configuration.setExposedHeaders(List.of(RequestHeaders.CORRELATION_ID));
    configuration.setMaxAge(Duration.ofHours(1));
    // Chrome's Private/Local Network Access: a page served from a public origin calling
    // localhost or a private IP sends "Access-Control-Request-Private-Network: true" on the
    // preflight and blocks the request (shown as a CORS error with no response headers)
    // unless the response grants it.
    configuration.setAllowPrivateNetwork(true);

    UrlBasedCorsConfigurationSource source = new UrlBasedCorsConfigurationSource();
    source.registerCorsConfiguration("/**", configuration);

    log.info(
        "CORS_CONFIGURED allowedOrigins={} credentials=true methods=* headers=*"
            + " privateNetwork=true exposedHeaders={}",
        origins.equals(List.of(ALLOW_ALL)) ? "* (any origin)" : origins,
        RequestHeaders.CORRELATION_ID);
    return new CorsWebFilter(source);
  }

  private static List<String> allowedOriginPatterns(List<String> configured) {
    List<String> origins =
        configured == null
            ? List.of()
            : configured.stream().map(String::trim).filter(o -> !o.isEmpty()).toList();
    return origins.isEmpty() || origins.contains(ALLOW_ALL) ? List.of(ALLOW_ALL) : origins;
  }
}
