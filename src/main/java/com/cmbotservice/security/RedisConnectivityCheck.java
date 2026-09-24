package com.cmbotservice.security;

import com.cmbotservice.common.LogSanitizer;
import java.time.Duration;
import java.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.data.redis.connection.ReactiveRedisConnection;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Once the app is ready, PINGs the session Redis in the background and logs the result — {@code
 * REDIS_SESSION_STORE_REACHABLE} or {@code REDIS_SESSION_STORE_UNREACHABLE} with the endpoint and
 * cause chain. This opens Lettuce's shared connection (including, with Entra ID auth, the token
 * fetch) at startup rather than on the first user request, so a bad host/TLS/credential shows up
 * immediately in the startup log and the first request doesn't pay the connect cost.
 *
 * <p>Deliberately non-fatal and asynchronous: an unreachable Redis must not stop this service
 * starting (it has its own explicit handling — a 503, or {@code fail-open-on-redis-error} locally),
 * and the lookup path retries connecting on every request anyway.
 */
@Component
@ConditionalOnProperty(prefix = "chatbot.security", name = "mode", havingValue = "BFF_SESSION")
public class RedisConnectivityCheck {

  private static final Logger log = LoggerFactory.getLogger(RedisConnectivityCheck.class);

  private static final Duration TIMEOUT = Duration.ofSeconds(30);

  private final ReactiveRedisConnectionFactory connectionFactory;
  private final RedisEndpoint redisEndpoint;

  public RedisConnectivityCheck(
      ReactiveRedisConnectionFactory connectionFactory, RedisEndpoint redisEndpoint) {
    this.connectionFactory = connectionFactory;
    this.redisEndpoint = redisEndpoint;
  }

  @EventListener(ApplicationReadyEvent.class)
  public void checkOnStartup() {
    Instant start = Instant.now();
    log.info("REDIS_SESSION_STORE_CHECK_STARTED endpoint={}", redisEndpoint);
    Mono.usingWhen(
            // getReactiveConnection() connects synchronously (and fetches the Entra ID
            // token with a blocking call), so it must run on a blocking-capable thread.
            Mono.fromCallable(connectionFactory::getReactiveConnection)
                .subscribeOn(Schedulers.boundedElastic()),
            ReactiveRedisConnection::ping,
            ReactiveRedisConnection::closeLater)
        .timeout(TIMEOUT)
        .subscribe(
            pong ->
                log.info(
                    "REDIS_SESSION_STORE_REACHABLE endpoint={} reply={} durationMs={}",
                    redisEndpoint,
                    pong,
                    Duration.between(start, Instant.now()).toMillis()),
            ex ->
                log.error(
                    "REDIS_SESSION_STORE_UNREACHABLE endpoint={} durationMs={} causeChain=[{}] —"
                        + " session lookups will fail with 503 until this is fixed",
                    redisEndpoint,
                    Duration.between(start, Instant.now()).toMillis(),
                    LogSanitizer.causeChain(ex)));
  }
}
