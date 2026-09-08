package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.config.MlAgentProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.unit.DataSize;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.test.StepVerifier;

import java.io.IOException;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises {@link HttpMlAgentClient} against a real (local) HTTP server, proving the
 * shared {@code WebClient} plumbing, the real request body shape, and response
 * validation actually work end to end — not just that the code compiles.
 */
class HttpMlAgentClientTest {

    private MockWebServer server;
    private HttpMlAgentClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() throws IOException {
        server = new MockWebServer();
        server.start();
        WebClient webClient = WebClient.builder().baseUrl(server.url("/").toString()).build();
        MlAgentProperties properties = new MlAgentProperties(
                "http", server.url("/").toString(), "/v1/chat",
                Duration.ofSeconds(2), Duration.ofSeconds(5),
                Duration.ofSeconds(2), Duration.ofSeconds(2),
                10, Duration.ofSeconds(1), Duration.ofSeconds(30), Duration.ofMinutes(5),
                DataSize.ofKilobytes(256), true);
        client = new HttpMlAgentClient(webClient, properties, objectMapper);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
    }

    private static MlAgentRequest request() {
        return new MlAgentRequest("tenant-1", "case-1", "cont-0", "conv-0", "msg-1", "analyst-1", "gadi5",
                "corr-1", "req-1", MlAgentRequest.SURFACE_CASE_MANAGER, true, "hello");
    }

    private static final String VALID_PAYLOAD_JSON =
            "{\"answer\":\"Hello world\","
                    + "\"summary\":{\"narrative\":\"n\",\"keySignals\":[{\"signal\":\"s\",\"severity\":\"high\",\"citations\":[\"c1\"]}],"
                    + "\"entities\":[],\"timeline\":[]},"
                    + "\"suggestedResolution\":{\"mark\":\"S\",\"label\":\"Suspected Fraud\",\"confidence\":\"medium\",\"rationale\":\"r\"},"
                    + "\"citations\":[{\"id\":\"c1\",\"source\":\"APP_EVENT_LOG\",\"fields\":[\"risk_score\"]}]}";

    @Test
    void successfulSseResponse_isParsedIntoDomainEvents_andToolEventsAreConsumedSilently() {
        String body = "event: token\ndata: {\"delta\":\"Hello\"}\n\n"
                + "event: token\ndata: {\"delta\":\" world\"}\n\n"
                + "event: tool_call\ndata: {\"id\":\"t1\",\"name\":\"lookup\",\"args\":{}}\n\n"
                + "event: tool_result\ndata: {\"id\":\"t1\",\"ms\":12,\"rowCount\":3,\"ok\":true}\n\n"
                + "event: payload\ndata: " + VALID_PAYLOAD_JSON + "\n\n"
                + "event: done\ndata: {\"conversationId\":\"conv-1\",\"continuation\":\"cont-1\",\"latencyMs\":100,\"tokensIn\":5,\"tokensOut\":10}\n\n";
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody(body));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token t
                        && t.sequence() == 1 && t.delta().equals("Hello"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Token t
                        && t.sequence() == 2 && t.delta().equals(" world"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Payload p
                        && p.payload().answer().equals("Hello world")
                        && p.payload().suggestedResolution().mark().equals("S"))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Done d
                        && d.conversationId().equals("conv-1") && d.continuation().equals("cont-1"))
                .verifyComplete();
    }

    @Test
    void requestBody_matchesTheRealWireContract_andOmitsInternalOnlyFields() throws Exception {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: done\ndata: {\"conversationId\":\"conv-1\",\"continuation\":\"cont-1\",\"latencyMs\":1,\"tokensIn\":1,\"tokensOut\":1}\n\n"));

        client.streamResponse(request()).blockLast(Duration.ofSeconds(5));

        RecordedRequest recorded = server.takeRequest();
        assertThat(recorded.getPath()).isEqualTo("/v1/chat");
        JsonNode json = objectMapper.readTree(recorded.getBody().readUtf8());

        assertThat(json.get("continuation").asText()).isEqualTo("cont-0");
        assertThat(json.get("conversationId").asText()).isEqualTo("conv-0");
        assertThat(json.get("surface").asText()).isEqualTo("case_manager");
        assertThat(json.get("message").asText()).isEqualTo("hello");
        assertThat(json.get("tenantId").asText()).isEqualTo("tenant-1");
        assertThat(json.get("context").get("caseId").asText()).isEqualTo("case-1");
        assertThat(json.get("context").get("endUserId").asText()).isEqualTo("gadi5");
        assertThat(json.get("options").get("includeResolutions").asBoolean()).isTrue();

        assertThat(json.has("history")).isFalse();
        assertThat(json.has("contextToken")).isFalse();
        assertThat(json.has("messageId")).isFalse();
        assertThat(json.has("correlationId")).isFalse();
        assertThat(json.has("requestId")).isFalse();
        assertThat(json.has("userId")).isFalse();
    }

    @Test
    void connectionFailure_isMappedToMlAgentUnavailableException() throws IOException {
        server.shutdown(); // the connection itself fails before any response

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentUnavailableException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream401_isMappedToMlAgentRejectedException() {
        server.enqueue(new MockResponse().setResponseCode(401));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentRejectedException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream403_isMappedToMlAgentRejectedException() {
        server.enqueue(new MockResponse().setResponseCode(403));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentRejectedException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream404_isMappedToMlAgentRejectedException() {
        server.enqueue(new MockResponse().setResponseCode(404));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentRejectedException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream422_isMappedToMlAgentRejectedException() {
        server.enqueue(new MockResponse().setResponseCode(422));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentRejectedException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream429_isMappedToMlAgentCommunicationException() {
        server.enqueue(new MockResponse().setResponseCode(429));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentCommunicationException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void preStream503_isMappedToMlAgentCommunicationException() {
        server.enqueue(new MockResponse().setResponseCode(503));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentCommunicationException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void inStreamErrorEvent_code429_isMappedToMlAgentCommunicationException() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: error\ndata: {\"code\":\"429\",\"message\":\"slow down\",\"retryable\":true}\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentCommunicationException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void inStreamErrorEvent_code4221_isMappedToMlAgentContinuationExpiredException() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: error\ndata: {\"code\":\"4221\",\"message\":\"expired\",\"retryable\":false}\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentContinuationExpiredException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void inStreamErrorEvent_code4222_isMappedToMlAgentContinuationExpiredException() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: error\ndata: {\"code\":\"4222\",\"message\":\"tampered\",\"retryable\":false}\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentContinuationExpiredException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void malformedToken_missingDelta_isMappedToMalformedResponseException() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: token\ndata: {}\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void malformedPayload_keySignalMissingCitation_isMappedToMalformedResponseException() {
        String badPayload = "{\"answer\":\"a\",\"summary\":{\"narrative\":\"n\","
                + "\"keySignals\":[{\"signal\":\"s\",\"severity\":\"high\",\"citations\":[]}],"
                + "\"entities\":[],\"timeline\":[]},\"suggestedResolution\":null,\"citations\":[]}";
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: payload\ndata: " + badPayload + "\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void malformedPayload_unknownResolutionMark_isMappedToMalformedResponseException() {
        String badPayload = "{\"answer\":\"a\",\"summary\":null,"
                + "\"suggestedResolution\":{\"mark\":\"Z\",\"label\":\"?\",\"confidence\":\"low\",\"rationale\":\"?\"},"
                + "\"citations\":[]}";
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: payload\ndata: " + badPayload + "\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }

    @Test
    void unrecognizedEventType_isMappedToMalformedResponseException() {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "text/event-stream")
                .setBody("event: mystery\ndata: {}\n\n"));

        StepVerifier.create(client.streamResponse(request()))
                .expectNextMatches(e -> e instanceof MlAgentStreamEvent.Started)
                .expectError(MlAgentMalformedResponseException.class)
                .verify(Duration.ofSeconds(5));
    }
}
