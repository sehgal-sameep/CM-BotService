package com.cmbotservice.security;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * A log-safe description of the Redis instance the session lookup talks to — host, port, TLS, and
 * whether Entra ID passwordless auth is on (never the password itself). Logged once at startup and
 * attached to every Redis failure log, so "cannot connect to Redis" always says <i>which</i> Redis
 * and with what settings, instead of leaving you to reconstruct the effective configuration.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class RedisEndpoint {

  private static final Logger log = LoggerFactory.getLogger(RedisEndpoint.class);

  private final String description;

  @Autowired
  public RedisEndpoint(
      @Value("${spring.data.redis.host:localhost}") String host,
      @Value("${spring.data.redis.port:6379}") int port,
      @Value("${spring.data.redis.ssl.enabled:false}") boolean ssl,
      @Value("${spring.data.redis.azure.passwordless-enabled:false}") boolean passwordless,
      @Value("${spring.data.redis.password:}") String password) {
    String auth = passwordless ? "entra-id" : (password.isEmpty() ? "none" : "password");
    this.description = host + ":" + port + " (ssl=" + ssl + ", auth=" + auth + ")";
  }

  public static RedisEndpoint of(String description) {
    return new RedisEndpoint(description);
  }

  private RedisEndpoint(String description) {
    this.description = description;
  }

  @PostConstruct
  void logConfiguredEndpoint() {
    log.info("REDIS_SESSION_STORE_CONFIGURED endpoint={}", description);
  }

  public String description() {
    return description;
  }

  @Override
  public String toString() {
    return description;
  }
}
