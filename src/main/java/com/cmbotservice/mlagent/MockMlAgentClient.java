package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentCommunicationException;
import com.cmbotservice.common.MlAgentContinuationExpiredException;
import com.cmbotservice.common.MlAgentRejectedException;
import com.cmbotservice.common.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

/**
 * Stand-in for the real ML Agent, used until the ML team's API is available.
 * <p>
 * Fully reactive and cold: nothing runs — no thread, no timer — until the returned
 * {@code Flux} is subscribed, and cancelling that subscription (e.g. the
 * orchestrator's timeout firing, or a client disconnect) stops everything downstream
 * of it for free via Reactor's own cancellation propagation. Behavior is selected via
 * {@link MockScenario} keywords in the message text so every failure mode the real
 * integration must handle is reachable from Swagger/curl with plain text, without
 * polluting the request contract with mock-only fields.
 * <p>
 * Mirrors the real contract's shape: no conversation identifier is revealed until
 * {@link MlAgentStreamEvent.Done}, and a structured {@link MlAgentStreamEvent.Payload}
 * (with a satisfying citation on every key signal, and a known resolution mark) is
 * always sent before it, so the new SSE path is fully exercisable without a real agent.
 * <p>
 * Active whenever {@code ml-agent.mode} is {@code mock} (the default).
 */
@Component
@ConditionalOnProperty(prefix = "ml-agent", name = "mode", havingValue = "mock", matchIfMissing = true)
public class MockMlAgentClient implements MlAgentClient {

    private static final Logger log = LoggerFactory.getLogger(MockMlAgentClient.class);

    private static final String SUMMARY_RESPONSE =
            "This case was created because the transaction triggered multiple fraud "
                    + "indicators. The transaction amount was significantly higher than the "
                    + "customer's typical spending pattern. It also originated from a device "
                    + "and IP address not previously associated with this account. Two "
                    + "velocity rules and one geolocation rule were triggered as a result. "
                    + "The case is currently awaiting analyst review.";

    private static final String RULES_RESPONSE =
            "Two rules were triggered on this transaction. The first is a velocity rule, "
                    + "flagging an unusual number of transactions in a short time window. The "
                    + "second is a geolocation rule, flagging a mismatch between the "
                    + "transaction origin and the customer's known location history.";

    private static final String SUSPICION_RESPONSE =
            "The transaction was considered suspicious primarily due to its deviation "
                    + "from the customer's established behavior profile. The amount, "
                    + "merchant category, and originating device were all atypical for this "
                    + "account. Combined, these factors raised the transaction's risk score "
                    + "above the case-creation threshold.";

    private static final String NEXT_STEPS_RESPONSE =
            "A reasonable next step is to verify the transaction directly with the "
                    + "customer through an out-of-band channel. It is also worth reviewing "
                    + "recent account activity for other atypical transactions, and checking "
                    + "whether the device or IP address has appeared on prior cases.";

    private static final String GENERIC_RESPONSE =
            "Based on the information available for this case, the transaction shows "
                    + "several characteristics consistent with fraudulent activity. Further "
                    + "review of the linked account and rule history is recommended before "
                    + "reaching a final disposition.";

    private final Duration chunkDelay;
    private final Duration slowChunkDelay;

    public MockMlAgentClient(
            @Value("${app.mock-ml-agent.chunk-delay-ms}") long chunkDelayMs,
            @Value("${app.mock-ml-agent.slow-chunk-delay-ms}") long slowChunkDelayMs
    ) {
        this.chunkDelay = Duration.ofMillis(chunkDelayMs);
        this.slowChunkDelay = Duration.ofMillis(slowChunkDelayMs);
    }

    @Override
    public Flux<MlAgentStreamEvent> streamResponse(MlAgentRequest request) {
        MockScenario scenario = MockScenario.fromPrompt(request.message());
        log.debug("Mock ML Agent selected scenario={} for conversationId={}", scenario, request.conversationId());

        return switch (scenario) {
            case ERROR -> Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentCommunicationException("Simulated ML Agent failure (trigger:error)")));
            case REJECTED -> Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentRejectedException(ErrorCode.NOT_FOUND,
                            "Simulated rejection: case not found in that tenant (trigger:rejected)")));
            case CONTINUATION_EXPIRED -> Flux.concat(
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Flux.error(new MlAgentContinuationExpiredException(
                            "Simulated continuation expiry — start a new conversation (trigger:continuation-expired)")));
            case EMPTY -> {
                ResolvedContinuation resolved = resolveContinuation(request);
                yield Flux.just(
                        new MlAgentStreamEvent.Started(),
                        new MlAgentStreamEvent.Done(resolved.conversationId(), resolved.continuation(), 0, 0, 0));
            }
            case TIMEOUT -> Flux.concat(
                    // Deliberately hangs after Started — the orchestrator's own
                    // first-response/idle-timeout operator is what ends this stream;
                    // the mock has no polling loop of its own anymore.
                    Mono.just(new MlAgentStreamEvent.Started()),
                    Mono.never());
            case SLOW -> streamChunks(selectCannedResponse(request.message()), slowChunkDelay, request);
            default -> streamChunks(selectCannedResponse(request.message()), chunkDelay, request);
        };
    }

    private Flux<MlAgentStreamEvent> streamChunks(String text, Duration delay, MlAgentRequest request) {
        List<String> sentences = splitIntoSentences(text);
        Flux<MlAgentStreamEvent> tokens = Flux.fromIterable(sentences)
                .delayElements(delay)
                .index((sequence, sentence) -> new MlAgentStreamEvent.Token(sentence, sequence.intValue() + 1));
        ResolvedContinuation resolved = resolveContinuation(request);
        long approxTokensIn = request.message() == null ? 0 : Math.max(1, request.message().length() / 4);
        long approxTokensOut = sentences.size() * 10L;
        long approxLatencyMs = sentences.size() * delay.toMillis();
        return Flux.concat(
                Mono.just(new MlAgentStreamEvent.Started()),
                tokens,
                Mono.just(new MlAgentStreamEvent.Payload(buildPayload(text))),
                Mono.just(new MlAgentStreamEvent.Done(
                        resolved.conversationId(), resolved.continuation(), approxLatencyMs, approxTokensIn, approxTokensOut)));
    }

    /**
     * Echoes whatever the caller already had (continuation and/or conversationId), or
     * fabricates a plausible opaque pair if this is the first message of a new
     * conversation. A real ML Agent would do the equivalent against its own
     * conversation-memory store; this mock has none, so both are just tokens.
     */
    private static ResolvedContinuation resolveContinuation(MlAgentRequest request) {
        if (StringUtils.hasText(request.continuation()) || StringUtils.hasText(request.conversationId())) {
            return new ResolvedContinuation(request.conversationId(), request.continuation());
        }
        return new ResolvedContinuation("conv-" + UUID.randomUUID(), "v1.mock." + UUID.randomUUID());
    }

    private record ResolvedContinuation(String conversationId, String continuation) {
    }

    /**
     * Fabricates a structured payload that satisfies both invariants
     * {@link HttpMlAgentClient} validates on a real response — every key signal
     * carries a citation, and the resolution mark is one of the known codes — so the
     * mock exercises the exact same downstream path a real response would.
     */
    private static CaseSummaryPayload buildPayload(String answer) {
        String citationId = "MOCK-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase(Locale.ROOT);
        CaseSummaryPayload.KeySignal signal = new CaseSummaryPayload.KeySignal(
                "Transaction deviates from the customer's typical behavior profile", "high", List.of(citationId));
        CaseSummaryPayload.Entity entity = new CaseSummaryPayload.Entity("ip", "203.0.113.42", List.of(citationId));
        CaseSummaryPayload.TimelineEvent timelineEvent = new CaseSummaryPayload.TimelineEvent(
                java.time.Instant.now().toString(), citationId, "Transaction flagged by fraud detection rules");
        CaseSummaryPayload.Summary summary = new CaseSummaryPayload.Summary(
                answer, List.of(signal), List.of(entity), List.of(timelineEvent));
        CaseSummaryPayload.SuggestedResolution resolution = new CaseSummaryPayload.SuggestedResolution(
                "S", "Suspected Fraud", "medium",
                "Multiple fraud indicators triggered with no clear legitimate explanation.");
        CaseSummaryPayload.Citation citation = new CaseSummaryPayload.Citation(
                citationId, "APP_EVENT_LOG", List.of("risk_score"));
        return new CaseSummaryPayload(answer, summary, resolution, List.of(citation));
    }

    private static List<String> splitIntoSentences(String text) {
        return List.of(text.split("(?<=[.])\\s+"));
    }

    private static String selectCannedResponse(String message) {
        String lower = message == null ? "" : message.toLowerCase(Locale.ROOT);
        if (lower.contains("summar")) {
            return SUMMARY_RESPONSE;
        }
        if (lower.contains("rule")) {
            return RULES_RESPONSE;
        }
        if (lower.contains("suspicious") || lower.contains("unusual")) {
            return SUSPICION_RESPONSE;
        }
        if (lower.contains("next") || lower.contains("investigate")) {
            return NEXT_STEPS_RESPONSE;
        }
        return GENERIC_RESPONSE;
    }
}
