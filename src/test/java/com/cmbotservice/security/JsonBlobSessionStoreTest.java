package com.cmbotservice.security;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisConnectionException;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Verifies {@link JsonBlobSessionStore}'s parsing of the confirmed FMC-PM-BFF session envelope
 * ({@code context_json}/{@code access_token}/{@code refresh_token}/{@code fp}, with {@code
 * context_json} itself a nested JSON string) against a mocked {@link ReactiveStringRedisTemplate} —
 * no real Redis needed. This store performs no validation beyond "is there a well-formed record" —
 * see {@link SessionAuthenticationWebFilterTest} for the (currently minimal) authentication flow
 * built on top of it.
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
    store =
        new JsonBlobSessionStore(
            redisTemplate, properties(), objectMapper, RedisEndpoint.of("test-redis:6379"));
  }

  private static SecurityProperties properties() {
    return new SecurityProperties(
        ChatbotSecurityMode.BFF_SESSION,
        new SecurityProperties.Session("SESSION", "tenant"),
        new SecurityProperties.Redis(
            "json-blob",
            "session",
            new SecurityProperties.Redis.FieldNames(
                "context_json", "access_token", "refresh_token", "fp")),
        new SecurityProperties.Cors(List.of()),
        false);
  }

  private static String envelope(
      String contextJson, String accessToken, String refreshToken, String fp) {
    return "{\"context_json\":"
        + quoteAndEscape(contextJson)
        + ",\"access_token\":\""
        + accessToken
        + "\",\"refresh_token\":\""
        + refreshToken
        + "\",\"fp\":\""
        + fp
        + "\"}";
  }

  private static String quoteAndEscape(String rawJson) {
    return "\"" + rawJson.replace("\"", "\\\"") + "\"";
  }

  @Test
  void redisCall_isSubscribedOffTheEventLoop_soBlockingCredentialFetchesAreAllowed() {
    // Regression: with Entra ID auth, Lettuce's lazy first connect calls
    // AzureRedisCredentials.getPassword(), which does Mono.block() for the token. That
    // happens synchronously when the Redis Mono is subscribed. Subscribed on a WebFlux
    // event-loop (non-blocking) thread, Reactor throws "block()/blockFirst()/blockLast()
    // are blocking, which is not supported in thread reactor-http-nio-N".
    String contextJson = "{\"username\":\"alice\",\"tenantId\":\"tenant-1\"}";
    String record = envelope(contextJson, "at-1", "rt-1", "fp-1");
    java.util.concurrent.atomic.AtomicReference<String> redisSubscriptionThread =
        new java.util.concurrent.atomic.AtomicReference<>();
    java.util.concurrent.atomic.AtomicBoolean blockingForbiddenThere =
        new java.util.concurrent.atomic.AtomicBoolean(true);
    when(valueOperations.get("session:abc123:tenant-1"))
        .thenReturn(
            // Runs when the Redis Mono is subscribed — where Lettuce's lazy connect (and
            // the Entra ID token fetch's block()) happens in production.
            Mono.fromCallable(
                () -> {
                  redisSubscriptionThread.set(Thread.currentThread().getName());
                  blockingForbiddenThere.set(
                      reactor.core.scheduler.Schedulers.isInNonBlockingThread());
                  return record;
                }));

    // Subscribe from a non-blocking thread, exactly like a request on reactor-http-nio.
    Mono<SessionContext> lookup =
        Mono.defer(() -> store.findSession("abc123", "tenant-1"))
            .subscribeOn(reactor.core.scheduler.Schedulers.parallel());

    StepVerifier.create(lookup)
        .expectNextMatches(session -> session.username().equals("alice"))
        .verifyComplete();
    org.assertj.core.api.Assertions.assertThat(blockingForbiddenThere.get())
        .as("Redis call subscribed on %s, where block() is forbidden", redisSubscriptionThread)
        .isFalse();
  }

  @Test
  void sessionFound_parsesEnvelopeAndNestedContextJson() {
    String contextJson = "{\"username\":\"alice\",\"tenantId\":\"tenant-1\"}";
    when(valueOperations.get("session:abc123:tenant-1"))
        .thenReturn(Mono.just(envelope(contextJson, "at-1", "rt-1", "fp-1")));

    StepVerifier.create(store.findSession("abc123", "tenant-1"))
        .expectNextMatches(
            ctx ->
                ctx.username().equals("alice")
                    && ctx.tenantId().equals("tenant-1")
                    && ctx.accessToken().equals("at-1")
                    && ctx.refreshToken().equals("rt-1")
                    && ctx.contextJson().equals(contextJson)
                    && ctx.fingerprint().equals("fp-1"))
        .verifyComplete();
  }

  @Test
  void missingRefreshTokenOrFingerprint_isToleratedAsNull() {
    String contextJson = "{\"username\":\"alice\",\"tenantId\":\"tenant-1\"}";
    when(valueOperations.get(anyString()))
        .thenReturn(
            Mono.just(
                "{\"context_json\":"
                    + quoteAndEscape(contextJson)
                    + ",\"access_token\":\"at-1\"}"));

    StepVerifier.create(store.findSession("abc123", "tenant-1"))
        .expectNextMatches(ctx -> ctx.refreshToken() == null && ctx.fingerprint() == null)
        .verifyComplete();
  }

  @Test
  void blankContextJson_isTreatedAsNotFound() {
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.just("{\"context_json\":\"\",\"access_token\":\"at-1\"}"));

    StepVerifier.create(store.findSession("abc123", "tenant-1")).verifyComplete();
  }

  @Test
  void blankAccessToken_isTreatedAsNotFound() {
    String contextJson = "{\"username\":\"alice\",\"tenantId\":\"tenant-1\"}";
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.just(envelope(contextJson, "", "rt-1", "fp-1")));

    StepVerifier.create(store.findSession("abc123", "tenant-1")).verifyComplete();
  }

  @Test
  void blankUsernameInContextJson_isTreatedAsNotFound() {
    String contextJson = "{\"tenantId\":\"tenant-1\"}";
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.just(envelope(contextJson, "at-1", "rt-1", "fp-1")));

    StepVerifier.create(store.findSession("abc123", "tenant-1")).verifyComplete();
  }

  @Test
  void noRecordAtKey_returnsEmpty() {
    when(valueOperations.get(anyString())).thenReturn(Mono.empty());

    StepVerifier.create(store.findSession("missing", "tenant-1")).verifyComplete();
  }

  @Test
  void malformedJson_isTreatedAsNotFoundRatherThanPropagatingAnError() {
    when(valueOperations.get(anyString())).thenReturn(Mono.just("this is not json"));

    StepVerifier.create(store.findSession("abc123", "tenant-1")).verifyComplete();
  }

  @Test
  void malformedNestedContextJson_isTreatedAsNotFoundRatherThanPropagatingAnError() {
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.just("{\"context_json\":\"not json\",\"access_token\":\"at-1\"}"));

    StepVerifier.create(store.findSession("abc123", "tenant-1")).verifyComplete();
  }

  @Test
  void redisConnectionFailure_propagatesAsAnErrorRatherThanEmpty() {
    when(valueOperations.get(anyString()))
        .thenReturn(Mono.error(new RedisConnectionException("connection refused")));

    StepVerifier.create(store.findSession("abc123", "tenant-1"))
        .expectError(RedisConnectionException.class)
        .verify();
  }

  @Test
  void keyIsNamespacePrefixedSessionIdAndTenant() {
    when(valueOperations.get(anyString())).thenReturn(Mono.empty());

    store.findSession("abc123", "tenant-1").block();

    org.mockito.Mockito.verify(valueOperations).get("session:abc123:tenant-1");
  }
}
