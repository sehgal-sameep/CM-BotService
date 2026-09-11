package com.cmbotservice.mlagent;

import java.util.List;

/**
 * The structured "Case Manager surface" object the real ML Agent sends as its single
 * {@code payload} event, exactly once, before {@code done}. Two invariants the agent's
 * own contract guarantees but which {@link CaseSummaryPayloadValidator} still
 * validates rather than trusting blindly (consistent with this service's "never trust
 * the agent blindly" stance):
 * <ul>
 *   <li>every {@link KeySignal} carries at least one citation — the contract states a
 *       signal without a resolvable citation is a defect, not a soft failure</li>
 *   <li>{@link SuggestedResolution#mark()}, when present, is always one of the known
 *       resolution enum names — the contract states the agent never invents one</li>
 * </ul>
 */
public record CaseSummaryPayload(
        String answer,
        Summary summary,
        SuggestedResolution suggestedResolution,
        List<Citation> citations
) {

    /**
     * The resolution enum names the real contract's Case Manager interface uses for
     * {@link SuggestedResolution#mark()}. Deliberately excludes {@code ANY}: the
     * contract documents it as "filter-only" and states it must never be emitted by
     * the agent, so its presence here would be a contract violation, not a legitimate
     * value to accept. Also deliberately excludes the single-character codes
     * ({@code F S G A U Y B T}) — those are a *different* system's
     * ({@code APP_EVENT_UPDATE.CUSTOM_MARK}) internal representation and, per the
     * contract, never appear on this interface.
     */
    public static final List<String> KNOWN_RESOLUTION_MARKS =
            List.of("CONFIRMED_FRAUD", "SUSPECTED_FRAUD", "CONFIRMED_GENUINE", "ASSUMED_GENUINE", "UNKNOWN");

    public record Summary(
            String narrative,
            List<KeySignal> keySignals,
            List<Entity> entities,
            List<TimelineEvent> timeline
    ) {
    }

    public record KeySignal(String signal, String severity, List<String> citations) {
    }

    public record Entity(String type, String value, List<String> events) {
    }

    public record TimelineEvent(String at, String eventId, String what) {
    }

    public record SuggestedResolution(String mark, String label, String confidence, String rationale) {
    }

    public record Citation(String id, String source, List<String> fields) {
    }
}
