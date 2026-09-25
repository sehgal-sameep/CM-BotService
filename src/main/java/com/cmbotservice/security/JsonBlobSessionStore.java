package com.cmbotservice.security;

import com.cmbotservice.common.LogSanitizer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

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
 *
 * <p>Every lookup is logged at INFO (started, found/parsed with duration), every "no usable record"
 * outcome at WARN with the specific reason, and a Redis failure at ERROR with the endpoint and the
 * full cause chain. The session id only ever appears masked ({@link LogSanitizer#maskSecret}); the
 * record's raw content and tokens never appear at all.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
@ConditionalOnProperty(
    prefix = "chatbot.security.redis",
    name = "strategy",
    havingValue = "json-blob",
    matchIfMissing = true)
@Slf4j
public class JsonBlobSessionStore implements SessionStore {

  private final ReactiveStringRedisTemplate redisTemplate;
  private final SecurityProperties properties;
  private final ObjectMapper objectMapper;
  private final RedisEndpoint redisEndpoint;

  public JsonBlobSessionStore(
      ReactiveStringRedisTemplate redisTemplate,
      SecurityProperties properties,
      ObjectMapper objectMapper,
      RedisEndpoint redisEndpoint) {
    this.redisTemplate = redisTemplate;
    this.properties = properties;
    this.objectMapper = objectMapper;
    this.redisEndpoint = redisEndpoint;
  }

  @Override
  public Mono<SessionContext> findSession(String sessionCookieValue, String tenant) {
    String namespace = properties.redis().namespace();
    String key = namespace + ":" + sessionCookieValue + ":" + tenant;
    String loggableKey =
        namespace + ":" + LogSanitizer.maskSecret(sessionCookieValue) + ":" + tenant;
    return Mono.defer(
        () -> {
          Instant start = Instant.now();
          log.info("REDIS_SESSION_LOOKUP_STARTED key={} endpoint={}", loggableKey, redisEndpoint);
          return redisTemplate
              .opsForValue()
              .get(key)
              // Subscribe off the WebFlux event loop. Spring Data Redis opens the shared
              // Lettuce connection lazily and synchronously on first use (and again after
              // a reconnect), inside this subscription. With Entra ID auth that connect
              // calls AzureRedisCredentials.getPassword(), which does Mono.block() for the
              // token — forbidden on reactor-http-nio threads ("block()/blockFirst()/
              // blockLast() are blocking, which is not supported in thread ..."). Even
              // with a plain password, the TCP+TLS connect would stall the event loop.
              // Once connected, Lettuce's commands are non-blocking; the hop is cheap.
              .subscribeOn(Schedulers.boundedElastic())
              .doOnNext(
                  json ->
                      log.info(
                          "REDIS_SESSION_RECORD_FOUND key={} recordLength={} durationMs={}",
                          loggableKey,
                          json.length(),
                          elapsedMs(start)))
              .switchIfEmpty(Mono.fromRunnable(() -> logNotFound(loggableKey, start)))
              .doOnError(ex -> logLookupFailure(loggableKey, start, ex))
              .flatMap(json -> parseOrEmpty(json, loggableKey));
        });
  }

  private static void logNotFound(String loggableKey, Instant start) {
    log.warn(
        "REDIS_SESSION_RECORD_NOT_FOUND key={} durationMs={} hint=check the session value, that"
            + " the tenant matches the session's tenant, and that this service reads the same Redis"
            + " as the BFF",
        loggableKey,
        elapsedMs(start));
  }

  private void logLookupFailure(String loggableKey, Instant start, Throwable ex) {
    log.error(
        "REDIS_SESSION_LOOKUP_FAILED key={} endpoint={} durationMs={} causeChain=[{}]",
        loggableKey,
        redisEndpoint,
        elapsedMs(start),
        LogSanitizer.causeChain(ex),
        ex);
  }

  private static long elapsedMs(Instant start) {
    return Duration.between(start, Instant.now()).toMillis();
  }

  /**
   * A corrupt/unparseable/incomplete record is treated the same as "no record found" (empty {@code
   * Mono}) rather than propagated as an error — it isn't a store outage, so it shouldn't be handled
   * like one (see {@link SessionAuthenticationWebFilter}'s distinct handling of empty vs. error).
   * Logged at WARN with the reason, never including the raw record content.
   */
  private Mono<SessionContext> parseOrEmpty(String json, String loggableKey) {
    try {
      SessionContext parsed = parse(json, loggableKey);
      if (parsed == null) {
        return Mono.empty();
      }
      log.info(
          "REDIS_SESSION_RECORD_PARSED key={} username={} tenantId={} refreshTokenPresent={}"
              + " fingerprintPresent={}",
          loggableKey,
          parsed.username(),
          parsed.tenantId(),
          StringUtils.hasText(parsed.refreshToken()),
          StringUtils.hasText(parsed.fingerprint()));
      return Mono.just(parsed);
    } catch (RuntimeException ex) {
      log.warn(
          "REDIS_SESSION_RECORD_UNPARSEABLE key={} reason={} result=treated as not found",
          loggableKey,
          ex.getMessage());
      return Mono.empty();
    }
  }

  private SessionContext parse(String json, String loggableKey) {
    SecurityProperties.Redis.FieldNames fields = properties.redis().fieldNames();
    JsonNode envelope = readTree(json);

    String contextJson = textOrNull(envelope, fields.contextJson());
    String accessToken = textOrNull(envelope, fields.accessToken());
    String refreshToken = textOrNull(envelope, fields.refreshToken());
    String fingerprint = textOrNull(envelope, fields.fingerprint());
    if (!StringUtils.hasText(contextJson) || !StringUtils.hasText(accessToken)) {
      log.warn(
          "REDIS_SESSION_RECORD_INCOMPLETE key={} contextJsonPresent={} accessTokenPresent={}"
              + " expectedFields={},{} (chatbot.security.redis.field-names) result=treated as not"
              + " found",
          loggableKey,
          StringUtils.hasText(contextJson),
          StringUtils.hasText(accessToken),
          fields.contextJson(),
          fields.accessToken());
      return null;
    }

    JsonNode context = readTree(contextJson);
    String username = textOrNull(context, "username");
    if (!StringUtils.hasText(username)) {
      log.warn(
          "REDIS_SESSION_RECORD_INCOMPLETE key={} reason=no username in context_json"
              + " result=treated as not found",
          loggableKey);
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
