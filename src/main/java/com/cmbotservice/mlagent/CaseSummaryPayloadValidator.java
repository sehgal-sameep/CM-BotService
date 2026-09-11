package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentMalformedResponseException;

/**
 * Enforces the two invariants the real ML Agent contract states but doesn't guarantee
 * for free: every key signal carries a citation ("a signal without a resolvable
 * citation is a defect, not a soft failure"), and {@code suggestedResolution.mark} —
 * when present — is one of the known resolution enum names ("the agent never invents
 * one"). Extracted as its own class (rather than living inline in
 * {@link GrpcMlAgentClient}) since it's pure, transport-independent logic operating on
 * the domain {@link CaseSummaryPayload}, not on any wire format.
 */
public final class CaseSummaryPayloadValidator {

    private CaseSummaryPayloadValidator() {
    }

    public static void validate(CaseSummaryPayload payload) {
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
}
