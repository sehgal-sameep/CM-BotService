package com.cmbotservice.config;

import io.netty.channel.ChannelOption;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;
import reactor.netty.resources.ConnectionProvider;

/**
 * The single shared {@code WebClient} used for every ML Agent call
 * ({@link com.cmbotservice.mlagent.HttpMlAgentClient}) — built once at startup, never
 * per-request, with an explicit connection pool and timeout configuration instead of
 * relying on defaults.
 * <p>
 * Built from the <b>Boot-autoconfigured {@code WebClient.Builder} bean</b> (injected),
 * not {@code WebClient.builder()} directly — the autoconfigured builder already carries
 * Boot's Micrometer Observation instrumentation, which is what gives the ML Agent call
 * its own trace span (§14) for free. Constructing a fresh builder here would silently
 * lose that.
 * <p>
 * Only created in {@code ml-agent.mode: http}; the mock client has no network phase to
 * configure a connection pool for.
 */
@Configuration
@ConditionalOnProperty(prefix = "ml-agent", name = "mode", havingValue = "http")
public class MlAgentWebClientConfig {

    @Bean
    public WebClient mlAgentWebClient(WebClient.Builder builder, MlAgentProperties properties) {
        ConnectionProvider connectionProvider = ConnectionProvider.builder("ml-agent")
                .maxConnections(properties.maxConnections())
                .pendingAcquireTimeout(properties.pendingAcquireTimeout())
                .maxIdleTime(properties.maxIdleTime())
                .maxLifeTime(properties.maxLifeTime())
                .build();

        HttpClient httpClient = HttpClient.create(connectionProvider)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) properties.connectTimeout().toMillis())
                .responseTimeout(properties.responseTimeout())
                .keepAlive(true);

        return builder
                .baseUrl(properties.baseUrl())
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .codecs(configurer -> configurer.defaultCodecs()
                        .maxInMemorySize((int) properties.maxInMemorySize().toBytes()))
                .build();
    }
}
