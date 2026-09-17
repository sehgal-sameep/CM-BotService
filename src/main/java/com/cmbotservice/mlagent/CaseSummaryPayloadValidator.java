package com.cmbotservice.mlagent;

import com.cmbotservice.common.MlAgentMalformedResponseException;

/**
 * Enforces the one invariant the real ML Agent contract states but doesn't guarantee
 * for free: every key signal carries a citation ("a signal without a resolvable
 * citation is a defect, not a soft failure"). Extracted as its own class (rather than
 * living inline in {@link GrpcMlAgentClient}) since it's pure, transport-independent
 * logic operating on the domain {@link CaseSummaryPayload}, not on any wire format.
 */
public final class CaseSummaryPayloadValidator {

    private CaseSummaryPayloadValidator() {
    }

    public static void validate(CaseSummaryPayload payload) {
        if (payload == null) {
            throw new MlAgentMalformedResponseException("ML Agent 'payload' event is missing its data");
        }
        if (payload.keySignals() != null) {
            for (CaseSummaryPayload.KeySignal signal : payload.keySignals()) {
                if (signal.citations() == null || signal.citations().isEmpty()) {
                    throw new MlAgentMalformedResponseException(
                            "ML Agent 'payload' key signal '" + signal.signal() + "' is missing a required citation");
                }
            }
        }
    }
}
