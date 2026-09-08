package com.cmbotservice.mlagent;

import com.cmbotservice.common.ErrorCode;
import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentMalformedResponseException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.MlAgentUnavailableException;
import com.cmbotservice.config.MlAgentProperties;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SynchronousSink;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Real {@link MlAgentClient} implementation: calls Thoughtful Labs' {@code POST
 * /v1/chat} over the shared, pooled {@code WebClient} from
 * {@link com.cmbotservice.config.MlAgentWebClientConfig}, and translates its six SSE
 * event types (<code>token</code>, <code>tool_call</code>, <code>tool_result</code>,
 * <code>payload</code>, <code>done</code>, <code>error</code>) into
 * {@link MlAgentStreamEvent}. Never buffers the response — no
 * {@code collectList()}/{@code .block()}, just a straight {@code Flux}.
 * <p>
 * {@code tool_call}/{@code tool_result} are consumed and logged only, per the real
 * contract's own note that they're "rendered in the sandbox trace, logged in
 * product" — this backend is the product, not the sandbox, so they never become a
 * domain event.
 * <p>
 * Selected via {@code ml-agent.mode: http}; {@link MockMlAgentClient} steps aside
 * automatically ({@code @ConditionalOnProperty} on both, never a runtime check here).
 */
@Component
@ConditionalOnProperty(prefix = "ml-agent", name = "mode", havingValue = "http")
public class HttpMlAgentClient implements MlAgentClient {

    private static final Logger log = LoggerFactory.getLogger(HttpMlAgentClient.class);

    private static final ParameterizedTypeReference<ServerSentEvent<String>> SSE_TYPE =
            new ParameterizedTypeReference<>() {
            };

    private final WebClient webClient;
    private final String chatPath;
    private final ObjectMapper objectMapper;

    public HttpMlAgentClient(WebClient mlAgentWebClient, MlAgentProperties properties, ObjectMapper objectMapper) {
        this.webClient = mlAgentWebClient;
        this.chatPath = properties.chatPath();
        this.objectMapper = objectMapper;
    }

    @Override
    public Flux<MlAgentStreamEvent> streamResponse(MlAgentRequest request) {
        AtomicInteger lastSequence = new AtomicInteger(0);
        ChatRequestBody body = toRequestBody(request);

        Flux<MlAgentStreamEvent> events = webClient.post()
                .uri(chatPath)
                .contentType(MediaType.APPLICATION_JSON)
                .accept(MediaType.TEXT_EVENT_STREAM)
                .bodyValue(body)
                .retrieve()
                .bodyToFlux(SSE_TYPE)
                .<MlAgentStreamEvent>handle((sse, sink) -> emit(sse, lastSequence, sink))
                .onErrorMap(WebClientRequestException.class,
                        ex -> new MlAgentUnavailableException("Could not reach the ML Agent", ex))
                .onErrorMap(WebClientResponseException.class, this::mapResponseStatus);

        return Flux.concat(Mono.just(new MlAgentStreamEvent.Started()), events);
    }

    private void emit(ServerSentEvent<String> sse, AtomicInteger lastSequence, SynchronousSink<MlAgentStreamEvent> sink) {
        String eventName = sse.event() == null ? "" : sse.event();
        String data = sse.data();
        switch (eventName) {
            case "token" -> sink.next(toToken(data, lastSequence));
            case "tool_call" -> logToolCall(data);
            case "tool_result" -> logToolResult(data);
            case "payload" -> sink.next(toPayload(data));
            case "done" -> sink.next(toDone(data));
            case "error" -> sink.error(toErrorException(data));
            default -> sink.error(new MlAgentMalformedResponseException("Unrecognized ML Agent event type: " + eventName));
        }
    }

    private MlAgentStreamEvent.Token toToken(String data, AtomicInteger lastSequence) {
        TokenPayload token = parse(data, TokenPayload.class);
        if (token == null || token.delta() == null) {
            throw new MlAgentMalformedResponseException("ML Agent 'token' event is missing 'delta'");
        }
        return new MlAgentStreamEvent.Token(token.delta(), lastSequence.incrementAndGet());
    }

    private MlAgentStreamEvent.Payload toPayload(String data) {
        CaseSummaryPayload payload = parse(data, CaseSummaryPayload.class);
        validatePayload(payload);
        return new MlAgentStreamEvent.Payload(payload);
    }

    /**
     * Enforces the two invariants the real contract states but doesn't guarantee for
     * free: every key signal carries a citation ("a signal without a resolvable
     * citation is a defect, not a soft failure"), and {@code suggestedResolution.mark}
     * — when present — is one of the known platform resolution codes ("the agent
     * never invents a label").
     */
    private static void validatePayload(CaseSummaryPayload payload) {
        if (payload == null) {
            throw new MlAgentMalformedResponseException("ML Agent 'payload' event is missing its data");
        }
        if (payload.summary() != null && payload.summary().keySignals() != null) {
            for (CaseSummaryPayload.KeySignal signal : payload.summary().keySignals()) {
                if (signal.citations() == null || signal.citations().isEmpty()) {
                    throw new MlAgentMalformedResponseException(
                            "ML Agent 'payload' key signal '" + signal.signal() + "' is missing a required citation");
                }
            }
        }
        CaseSummaryPayload.SuggestedResolution resolution = payload.suggestedResolution();
        if (resolution != null && resolution.mark() != null
                && !CaseSummaryPayload.KNOWN_RESOLUTION_MARKS.contains(resolution.mark())) {
            throw new MlAgentMalformedResponseException(
                    "ML Agent 'payload' suggestedResolution.mark is not a known resolution code: " + resolution.mark());
        }
    }

    private MlAgentStreamEvent.Done toDone(String data) {
        DonePayload done = parse(data, DonePayload.class);
        if (done == null) {
            throw new MlAgentMalformedResponseException("ML Agent 'done' event is missing its payload");
        }
        return new MlAgentStreamEvent.Done(
                done.conversationId(),
                done.continuation(),
                orZero(done.latencyMs()),
                orZero(done.tokensIn()),
                orZero(done.tokensOut()));
    }

    private Throwable toErrorException(String data) {
        ErrorPayload error = parse(data, ErrorPayload.class);
        if (error == null || error.code() == null) {
            return new MlAgentMalformedResponseException("ML Agent 'error' event is missing its code");
        }
        String message = error.message() == null ? "The ML Agent reported an error" : error.message();
        return mapErrorCode(error.code(), message);
    }

    /**
     * One unified error-code space the real contract uses both as the initial POST's
     * HTTP status (see {@link #mapResponseStatus}) and, for codes only discoverable
     * mid-stream, as this terminal {@code error} event's {@code code} field.
     */
    private static Throwable mapErrorCode(String code, String message) {
        return switch (code) {
            case "401", "403" -> new MlAgentRejectedException(ErrorCode.INTERNAL_ERROR, message);
            case "404" -> new MlAgentRejectedException(ErrorCode.NOT_FOUND, message);
            case "422" -> new MlAgentRejectedException(ErrorCode.VALIDATION_ERROR, message);
            case "429", "503" -> new MlAgentCommunicationException(message);
            case "4221", "4222" -> new MlAgentContinuationExpiredException(message);
            default -> new MlAgentCommunicationException(message);
        };
    }

    /**
     * 401/403/404/422/429/503 arrive as the HTTP status of the initial POST, before
     * any SSE byte is written — handled here, before {@link #emit} ever runs.
     */
    private Throwable mapResponseStatus(WebClientResponseException ex) {
        int status = ex.getStatusCode().value();
        return switch (status) {
            case 401, 403 -> new MlAgentRejectedException(ErrorCode.INTERNAL_ERROR,
                    "The ML Agent rejected our credentials or tenant access");
            case 404 -> new MlAgentRejectedException(ErrorCode.NOT_FOUND, "Case not found in that tenant");
            case 422 -> new MlAgentRejectedException(ErrorCode.VALIDATION_ERROR,
                    "Malformed request or missing required context");
            case 429, 503 -> new MlAgentCommunicationException(
                    "ML Agent is temporarily unavailable (status " + status + ")", ex);
            default -> new MlAgentCommunicationException("ML Agent returned an unexpected status " + status, ex);
        };
    }

    private void logToolCall(String data) {
        ToolCallPayload toolCall = parse(data, ToolCallPayload.class);
        if (toolCall != null) {
            log.debug("ML Agent tool_call id={} name={}", toolCall.id(), toolCall.name());
        }
    }

    private void logToolResult(String data) {
        ToolResultPayload toolResult = parse(data, ToolResultPayload.class);
        if (toolResult != null) {
            log.debug("ML Agent tool_result id={} ms={} rowCount={} ok={}",
                    toolResult.id(), toolResult.ms(), toolResult.rowCount(), toolResult.ok());
        }
    }

    private <T> T parse(String json, Class<T> type) {
        if (json == null) {
            return null;
        }
        try {
            return objectMapper.readValue(json, type);
        } catch (JsonProcessingException ex) {
            throw new MlAgentMalformedResponseException(
                    "Could not parse ML Agent '" + type.getSimpleName() + "' payload: " + ex.getMessage());
        }
    }

    private static long orZero(Long value) {
        return value == null ? 0L : value;
    }

    private static ChatRequestBody toRequestBody(MlAgentRequest request) {
        return new ChatRequestBody(
                request.continuation(),
                request.conversationId(),
                request.surface(),
                request.message(),
                new ChatRequestBody.Context(request.caseId(), request.endUserId()),
                request.tenantId(),
                new ChatRequestBody.Options(request.includeResolutions()));
    }

    /**
     * The literal wire shape {@code POST /v1/chat} expects — deliberately a separate,
     * private type from {@link MlAgentRequest}, which also carries internal-only
     * fields (messageId, correlationId, ...) that have no place on the wire. Omits
     * {@code history} (never used, see {@link MlAgentRequest}) and
     * {@code contextToken} (product/prod-auth path, not yet wired up — see
     * {@link MlAgentRequest#tenantId()}).
     */
    private record ChatRequestBody(
            String continuation,
            String conversationId,
            String surface,
            String message,
            Context context,
            String tenantId,
            Options options
    ) {
        private record Context(String caseId, String endUserId) {
        }

        private record Options(boolean includeResolutions) {
        }
    }

    private record TokenPayload(String delta) {
    }

    private record ToolCallPayload(String id, String name, Object args) {
    }

    private record ToolResultPayload(String id, Long ms, Long rowCount, Boolean ok) {
    }

    private record DonePayload(String conversationId, String continuation, Long latencyMs, Long tokensIn, Long tokensOut) {
    }

    private record ErrorPayload(String code, String message, Boolean retryable) {
    }
}
