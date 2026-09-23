package com.cmbotservice.security;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;

/**
 * Default, confirmed {@link SessionStore}: the FMC-PM-BFF session is one JSON document at key
 * {@code {namespace}:{sessionId}:{tenant}}, shaped as:
 *
 * <pre>{@code
 * { "context_json": "<JSON string>", "access_token": "<JWT>", "refresh_token": "...", "fp": "..." }
 * }</pre>
 *
 * {@code context_json} is itself a JSON string (not a nested object) carrying at least {@code
 * username}/{@code tenantId} — the only fields this service parses out of it. Every other envelope
 * field ({@code access_token}, {@code refresh_token}, {@code fp}) is carried through unchanged onto
 * {@link SessionContext} — this store does no validation of them beyond presence.
 *
 * <p><b>Current scope is deliberately minimal</b>: finding a well-formed record at the key is the
 * only condition for "authenticated" — no signature/expiry/fingerprint/CSRF check happens here or
 * in {@link SessionAuthenticationWebFilter}. Field names of the envelope are configurable ({@code
 * chatbot.security.redis.field-names.*}); the inner {@code context_json} shape is not, since it's a
 * fixed FMC-PM-BFF contract rather than a Redis-layout detail.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
@ConditionalOnProperty(
    prefix = "chatbot.security.redis",
    name = "strategy",
    havingValue = "json-blob",
    matchIfMissing = true)
public class JsonBlobSessionStore implements SessionStore {

  private static final Logger log = LoggerFactory.getLogger(JsonBlobSessionStore.class);

  private final ReactiveStringRedisTemplate redisTemplate;
  private final SecurityProperties properties;
  private final ObjectMapper objectMapper;

  public JsonBlobSessionStore(
      ReactiveStringRedisTemplate redisTemplate,
      SecurityProperties properties,
      ObjectMapper objectMapper) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    this.objectMapper = objectMapper;
  }

  @Override
  public Mono<SessionContext> findSession(String sessionCookieValue, String tenant) {
    String key = properties.redis().namespace() + ":" + sessionCookieValue + ":" + tenant;
    return redisTemplate.opsForValue().get(key).flatMap(this::parseOrEmpty);
  }

  /**
   * A corrupt/unparseable/incomplete record is treated the same as "no record found" (empty {@code
   * Mono}) rather than propagated as an error — it isn't a store outage, so it shouldn't be handled
   * like one (see {@link SessionAuthenticationWebFilter}'s distinct handling of empty vs. error).
   * Logged at WARN without ever including the raw record content.
   */
  private Mono<SessionContext> parseOrEmpty(String json) {
    try {
      SessionContext parsed = parse(json);
      return parsed == null ? Mono.empty() : Mono.just(parsed);
    } catch (RuntimeException ex) {
      log.warn(
          "Session record found but could not be parsed; treating as not found. reason={}",
          ex.getMessage());
      return Mono.empty();
    }
  }

  private SessionContext parse(String json) {
    SecurityProperties.Redis.FieldNames fields = properties.redis().fieldNames();
    JsonNode envelope = readTree(json);

    String contextJson = textOrNull(envelope, fields.contextJson());
    String accessToken = textOrNull(envelope, fields.accessToken());
    String refreshToken = textOrNull(envelope, fields.refreshToken());
    String fingerprint = textOrNull(envelope, fields.fingerprint());
    if (!StringUtils.hasText(contextJson) || !StringUtils.hasText(accessToken)) {
      return null;
    }

    JsonNode context = readTree(contextJson);
    String username = textOrNull(context, "username");
    if (!StringUtils.hasText(username)) {
      return null;
    }
    String tenantId = textOrNull(context, "tenantId");

    return new SessionContext(
        username, tenantId, accessToken, refreshToken, contextJson, fingerprint);
  }

  private JsonNode readTree(String json) {
    try {
      return objectMapper.readTree(json);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException("Session field is not valid JSON", ex);
    }
  }

  private static String textOrNull(JsonNode root, String fieldName) {
    JsonNode node = root.get(fieldName);
    return (node == null || node.isNull()) ? null : node.asText();
  }
}
