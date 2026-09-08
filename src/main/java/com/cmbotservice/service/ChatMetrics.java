package com.cmbotservice.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The only class in this service allowed to touch {@link MeterRegistry} directly.
 * Every method takes a fixed, low-cardinality argument (a small enum-like result
 * string) rather than exposing the registry itself — structurally impossible for a
 * caller to accidentally tag a metric with a high-cardinality value like
 * {@code conversationId}/{@code caseId}/{@code userId}, which Micrometer's own docs
 * warn can silently blow up a metrics backend.
 */
@Component
public class ChatMetrics {

    /** One chat request is exactly one SSE connection in this design, so the two metric families share this counter. */
    private final AtomicInteger activeConnections = new AtomicInteger();
    private final AtomicInteger activeMlStreams = new AtomicInteger();

    private final Counter chatRequestsTotal;
    private final Counter sseConnectionsCompleted;
    private final Counter sseConnectionsCancelled;
    private final Counter mlRequestsSuccess;
    private final Counter mlRequestsFailed;
    private final Counter mlRequestsTimeout;
    private final Counter mlRequestsRejected;
    private final Timer mlStreamDuration;
    private final Timer mlFirstResponseLatency;

    public ChatMetrics(MeterRegistry registry) {
        this.chatRequestsTotal = Counter.builder("chat.requests.total").register(registry);
        Gauge.builder("chat.requests.active", activeConnections, AtomicInteger::get).register(registry);
        Gauge.builder("sse.connections.active", activeConnections, AtomicInteger::get).register(registry);
        Gauge.builder("ml.stream.active", activeMlStreams, AtomicInteger::get).register(registry);

        this.sseConnectionsCompleted = Counter.builder("sse.connections.completed").register(registry);
        this.sseConnectionsCancelled = Counter.builder("sse.connections.cancelled").register(registry);

        this.mlRequestsSuccess = Counter.builder("ml.requests.total").tag("result", "success").register(registry);
        this.mlRequestsFailed = Counter.builder("ml.requests.total").tag("result", "failed").register(registry);
        this.mlRequestsTimeout = Counter.builder("ml.requests.total").tag("result", "timeout").register(registry);
        this.mlRequestsRejected = Counter.builder("ml.requests.total").tag("result", "rejected").register(registry);

        this.mlStreamDuration = Timer.builder("ml.stream.duration").register(registry);
        this.mlFirstResponseLatency = Timer.builder("ml.first_response.latency").register(registry);
    }

    public void connectionOpened() {
        chatRequestsTotal.increment();
        activeConnections.incrementAndGet();
    }

    public void connectionCompleted() {
        activeConnections.decrementAndGet();
        sseConnectionsCompleted.increment();
    }

    public void connectionCancelled() {
        activeConnections.decrementAndGet();
        sseConnectionsCancelled.increment();
    }

    public void mlStreamStarted() {
        activeMlStreams.incrementAndGet();
    }

    public void mlStreamEnded() {
        activeMlStreams.decrementAndGet();
    }

    public void mlRequestSucceeded() {
        mlRequestsSuccess.increment();
    }

    public void mlRequestFailed() {
        mlRequestsFailed.increment();
    }

    public void mlRequestTimedOut() {
        mlRequestsTimeout.increment();
    }

    public void mlRequestRejected() {
        mlRequestsRejected.increment();
    }

    public void recordStreamDuration(Duration duration) {
        mlStreamDuration.record(duration);
    }

    public void recordFirstResponseLatency(Duration duration) {
        mlFirstResponseLatency.record(duration);
    }
}
