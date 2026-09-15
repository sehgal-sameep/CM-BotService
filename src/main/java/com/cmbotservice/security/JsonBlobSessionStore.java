package com.cmbotservice.security;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

/**
 * Default {@link SessionStore}: treats the session as a single JSON document stored
 * at a plain string key, {@code {namespace}:{sessionCookieValue}}, read via
 * {@code GET} only.
 * <p>
 * <b>This is a best-effort default for a genuinely unconfirmed dependency.</b> The
 * diagrammed flow explicitly states the FMC-PM-BFF Redis session key format and
 * serialization are not yet confirmed, and instructs implementing this behind a
 * swappable strategy rather than blocking on it — this is that strategy
 * (`chatbot.security.redis.strategy: json-blob`, the default). Concretely, this
 * implementation assumes:
 * <ul>
 *   <li>the whole session is one JSON object at a plain string key (not, for example,
 *       Spring Session's default per-attribute Redis <em>hash</em> layout)</li>
 *   <li>the expiry field is either an ISO-8601 instant string or an epoch-millisecond
 *       number</li>
 *   <li>permission/organization fields are JSON arrays of strings</li>
 * </ul>
 * Field <em>names</em> are fully configurable ({@code chatbot.security.redis.field-names.*})
 * so a naming mismatch alone needs no code change. If FMC-PM-BFF's actual layout uses
 * a different structure entirely (e.g. Spring Session's hash-per-session-attribute
 * scheme), implement a new {@link SessionStore} bean guarded by
 * {@code @ConditionalOnProperty(chatbot.security.redis.strategy)} instead of changing
 * this class — {@link SessionAuthenticationWebFilter} depends only on the interface.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
@ConditionalOnProperty(prefix = "chatbot.security.redis", name = "strategy", havingValue = "json-blob", matchIfMissing = true)
public class JsonBlobSessionStore implements SessionStore {

    private static final Logger log = LoggerFactory.getLogger(JsonBlobSessionStore.class);

    private final ReactiveStringRedisTemplate redisTemplate;
    private final SecurityProperties properties;
    private final ObjectMapper objectMapper;

    public JsonBlobSessionStore(ReactiveStringRedisTemplate redisTemplate, SecurityProperties properties,
                                 ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<SessionContext> findSession(String sessionCookieValue) {
        String key = properties.redis().namespace() + ":" + sessionCookieValue;
        return redisTemplate.opsForValue().get(key)
                .flatMap(this::parseOrEmpty);
    }

    /**
     * A corrupt/unparseable record is treated the same as "no record found" (empty
     * {@code Mono}) rather than propagated as an error — it isn't a store outage, so
     * it shouldn't be handled like one (see {@link SessionAuthenticationWebFilter}'s
     * distinct handling of empty vs. error). Logged at WARN without ever including the
     * raw record content.
     */
    private Mono<SessionContext> parseOrEmpty(String json) {
        try {
            return Mono.just(parse(json));
        } catch (RuntimeException ex) {
            log.warn("Session record found but could not be parsed; treating as not found. reason={}", ex.getMessage());
            return Mono.empty();
        }
    }

    private SessionContext parse(String json) {
        JsonNode root;
        try {
            root = objectMapper.readTree(json);
        } catch (Exception ex) {
            throw new IllegalStateException("Session record is not valid JSON", ex);
        }
        SecurityProperties.Redis.FieldNames fields = properties.redis().fieldNames();
        return new SessionContext(
                textOrNull(root, fields.username()),
                textOrNull(root, fields.tenantId()),
                stringList(root, fields.permissions()),
                stringList(root, fields.organizations()),
                instantOrNull(root, fields.accessTokenExpiry()),
                textOrNull(root, fields.fingerprint())
        );
    }

    private static String textOrNull(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        return (node == null || node.isNull()) ? null : node.asText();
    }

    private static List<String> stringList(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || !node.isArray()) {
            return List.of();
        }
        List<String> values = new ArrayList<>(node.size());
        node.forEach(element -> values.add(element.asText()));
        return List.copyOf(values);
    }

    private static Instant instantOrNull(JsonNode root, String fieldName) {
        JsonNode node = root.get(fieldName);
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return Instant.ofEpochMilli(node.asLong());
        }
        try {
            return Instant.parse(node.asText());
        } catch (DateTimeParseException ex) {
            throw new IllegalStateException("Expiry field '" + fieldName + "' is neither an epoch-millis number nor an ISO-8601 instant", ex);
        }
    }
}
