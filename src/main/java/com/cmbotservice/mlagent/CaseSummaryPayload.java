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
 *       platform resolution codes — the contract states the agent never invents one</li>
 * </ul>
 */
public record CaseSummaryPayload(
        String answer,
        Summary summary,
        SuggestedResolution suggestedResolution,
        List<Citation> citations
) {

    /**
     * Platform resolution codes the real contract enumerates. {@code X}/{@code C} are
     * only valid when a tenant-side "ScamResolutions" flag is enabled — a flag this
     * backend has no visibility into — so both are accepted unconditionally rather
     * than risk wrongly rejecting a legitimate mark for a tenant with that flag on.
     */
    public static final List<String> KNOWN_RESOLUTION_MARKS = List.of("F", "S", "G", "A", "U", "Y", "B", "T", "X", "C");

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
