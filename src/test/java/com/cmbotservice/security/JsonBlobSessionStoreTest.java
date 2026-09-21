package com.cmbotservice.security;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisConnectionException;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Verifies {@link JsonBlobSessionStore}'s parsing of the assumed session JSON shape against a
 * mocked {@link ReactiveStringRedisTemplate} — no real Redis needed. See the class's own Javadoc
 * for the caveat this default strategy's layout is unconfirmed; these tests document exactly what
 * it currently assumes.
 */
@SuppressWarnings("unchecked")
class JsonBlobSessionStoreTest {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private ReactiveStringRedisTemplate redisTemplate;
  private ReactiveValueOperations<String, String> valueOperations;
  private JsonBlobSessionStore store;

  @BeforeEach
  void setUp() {
    redisTemplate = mock(ReactiveStringRedisTemplate.class);
    valueOperations = mock(ReactiveValueOperations.class);
    when(redisTemplate.opsForValue()).thenReturn(valueOperations);
    store = new JsonBlobSessionStore(redisTemplate, properties(), objectMapper);
  }

  private static SecurityProperties properties() {
    return new SecurityProperties(
        ChatbotSecurityMode.BFF_SESSION,
        new SecurityProperties.Session("SESSION", "X-Tenant-Id"),
        new SecurityProperties.Redis(
            "json-blob",
            "ns",
            "localhost",
            6379,
            false,
            "",
            new SecurityProperties.Redis.FieldNames(
                "username",
                "tenantId",
                "permissions",
                "organizations",
                "accessTokenExpiry",
                "fingerprint")),
        new SecurityProperties.Csrf(true, "XSRF-TOKEN", "X-XSRF-TOKEN"),
        new SecurityProperties.Authorization("CHATBOT_", false),
        new SecurityProperties.Cors(java.util.List.of()),
        false);
  }

  @Test
  void sessionFound_parsesAllConfiguredFieldsCorrectly() {
    String json =
        """
                {"username":"alice","tenantId":"tenant-1","permissions":["CHATBOT_CHAT","OTHER"],
                 "organizations":["org-1","org-2"],"accessTokenExpiry":"2030-01-01T00:00:00Z","fingerprint":"fp-1"}
                """;
    when(valueOperations.get("ns:abc123")).thenReturn(Mono.just(json));

    StepVerifier.create(store.findSession("abc123"))
        .expectNextMatches(
            ctx ->
                ctx.username().equals("alice")
                    && ctx.tenantId().equals("tenant-1")
                    && ctx.permissions().equals(java.util.List.of("CHATBOT_CHAT", "OTHER"))
                    && ctx.organizations().equals(java.util.List.of("org-1", "org-2"))
                    && ctx.accessTokenExpiry().equals(Instant.parse("2030-01-01T00:00:00Z"))
                    && ctx.fingerprint().equals("fp-1"))
        .verifyComplete();
  }

  @Test
  void expiryAsEpochMillisNumber_isAlsoAccepted() {
    long epochMillis = Instant.parse("2030-01-01T00:00:00Z").toEpochMilli();
    String json =
        "{\"username\":\"alice\",\"tenantId\":\"tenant-1\",\"permissions\":[],\"organizations\":[],"
            + "\"accessTokenExpiry\":"
            + epochMillis
            + "}";
    when(valueOperations.get("ns:abc123")).thenReturn(Mono.just(json));

    StepVerifier.create(store.findSession("abc123"))
        .expectNextMatches(ctx -> ctx.accessTokenExpiry().toEpochMilli() == epochMillis)
        .verifyComplete();
  }

  @Test
  void missingOptionalFields_areToleratedAsNullOrEmpty() {
    String json =
        "{\"username\":\"alice\",\"tenantId\":\"tenant-1\",\"accessTokenExpiry\":\"2030-01-01T00:00:00Z\"}";
    when(valueOperations.get("ns:abc123")).thenReturn(Mono.just(json));

    StepVerifier.create(store.findSession("abc123"))
        .expectNextMatches(
            ctx ->
                ctx.permissions().isEmpty()
                    && ctx.organizations().isEmpty()
                    && ctx.fingerprint() == null)
        .verifyComplete();
  }

  @Test
  void noRecordAtKey_returnsEmpty() {
    when(valueOperations.get(anyString())).thenReturn(Mono.empty());

    StepVerifier.create(store.findSession("missing")).verifyComplete();
  }

  @Test
  void malformedJson_isTreatedAsNotFoundRatherThanPropagatingAnError() {
    when(valueOperations.get(anyString())).thenReturn(Mono.just("this is not json"));

    StepVerifier.create(store.findSession("abc123")).verifyComplete();
  }

  @Test
  void unparsableExpiry_isTreatedAsNotFoundRatherThanPropagatingAnError() {
    String json =
        "{\"username\":\"alice\",\"tenantId\":\"tenant-1\",\"accessTokenExpiry\":\"not-a-date\"}";
    when(valueOperations.get(anyString())).thenReturn(Mono.just(json));

    StepVerifier.create(store.findSession("abc123")).verifyComplete();
  }

  @Test
  void redisConnectionFailure_propagatesAsAnErrorRatherThanEmpty() {
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.error(new RedisConnectionException("connection refused")));

    StepVerifier.create(store.findSession("abc123"))
        .expectError(RedisConnectionException.class)
        .verify();
  }

  @Test
  void keyIsNamespacePrefixedSessionCookieValue() {
    when(valueOperations.get(anyString())).thenReturn(Mono.empty());

    store.findSession("abc123").block();

    org.mockito.Mockito.verify(valueOperations).get("ns:abc123");
  }
}
