package com.cmbotservice.mlagent;

import java.util.List;

/**
 * The structured "Case Manager surface" object the real ML Agent sends as its single {@code
 * payload} event, exactly once, before {@code done}. One invariant the agent's own contract
 * guarantees but which {@link CaseSummaryPayloadValidator} still validates rather than trusting
 * blindly (consistent with this service's "never trust the agent blindly" stance): every {@link
 * KeySignal} carries at least one citation — the contract states a signal without a resolvable
 * citation is a defect, not a soft failure.
 */
public record CaseSummaryPayload(List<KeySignal> keySignals, List<Citation> citations) {

  public record KeySignal(String signal, List<String> citations) {}

  public record Citation(String id, String source, List<String> fields) {}
}
