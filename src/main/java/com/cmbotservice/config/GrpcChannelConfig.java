package com.cmbotservice.config;

import com.cmbotservice.mlagent.grpc.v1.ChatAgentGrpc;
import io.grpc.ManagedChannel;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The single shared gRPC {@link ManagedChannel} used for every ML Agent call ({@link
 * com.cmbotservice.mlagent.GrpcMlAgentClient}) — built once at startup, never per-request, same
 * principle as the WebClient/connection-pool setup this replaces.
 *
 * <p>Uses {@code grpc-netty-shaded} (see pom.xml) specifically so this channel's Netty usage can
 * never collide with the reactor-netty version WebFlux/the HTTP server already put on the classpath
 * — the shaded artifact relocates its own Netty classes under a private package for exactly this
 * reason.
 *
 * <p>Plaintext only (no TLS) — this is internal service-to-service traffic and TLS material is
 * infrastructure/secrets-management, explicitly out of scope here (same stance already taken for
 * the tracing exporter).
 *
 * <p>Only created in {@code ml-agent.mode: grpc}; the mock client has no network phase to open a
 * channel for.
 */
@Configuration
@ConditionalOnProperty(prefix = "ml-agent", name = "mode", havingValue = "grpc")
@Slf4j
public class GrpcChannelConfig {

  private ManagedChannel channel;

  @Bean
  public ManagedChannel mlAgentChannel(MlAgentProperties properties) {
    channel =
        NettyChannelBuilder.forAddress(properties.grpcHost(), properties.grpcPort())
            .usePlaintext()
            .maxInboundMessageSize((int) properties.grpcMaxInboundMessageSize().toBytes())
            .build();
    log.info(
        "ML_AGENT_GRPC_CHANNEL_CONFIGURED target={}:{} tls=false maxInboundMessageSize={}"
            + " firstResponseTimeout={} idleTimeout={}",
        properties.grpcHost(),
        properties.grpcPort(),
        properties.grpcMaxInboundMessageSize(),
        properties.firstResponseTimeout(),
        properties.idleTimeout());
    return channel;
  }

  @Bean
  public ChatAgentGrpc.ChatAgentStub chatAgentStub(ManagedChannel mlAgentChannel) {
    return ChatAgentGrpc.newStub(mlAgentChannel);
  }

  /**
   * Not strictly required for correctness (the JVM tearing down closes the sockets regardless), but
   * shuts the channel down cleanly rather than relying on that — same spirit as {@code
   * server.shutdown: graceful} elsewhere in this service.
   */
  @PreDestroy
  void shutdown() {
    if (channel != null) {
      log.info("ML_AGENT_GRPC_CHANNEL_SHUTDOWN target={}", channel.authority());
      channel.shutdown();
      try {
        channel.awaitTermination(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
