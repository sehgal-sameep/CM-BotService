package com.cmbotservice.mlagent;

import java.util.Locale;

/**
 * Behaviors {@link MockMlAgentClient} can simulate, selected by a keyword anywhere in
 * the analyst's message text (e.g. {@code "trigger:timeout"}). Keeping the trigger in
 * the message body — rather than a separate mock-only request field — keeps the public
 * request contract identical to what the real ML Agent will eventually receive, while
 * still making every failure mode trivially reproducible from Swagger or curl.
 */
public enum MockScenario {

    SUCCESS(null),
    SLOW("trigger:slow"),
    TIMEOUT("trigger:timeout"),
    ERROR("trigger:error"),
    EMPTY("trigger:empty"),
    REJECTED("trigger:rejected"),
    CONTINUATION_EXPIRED("trigger:continuation-expired");

    private final String keyword;

    MockScenario(String keyword) {
        this.keyword = keyword;
    }

    public static MockScenario fromPrompt(String prompt) {
        if (prompt == null) {
            return SUCCESS;
        }
        String lower = prompt.toLowerCase(Locale.ROOT);
        for (MockScenario scenario : values()) {
            if (scenario.keyword != null && lower.contains(scenario.keyword)) {
                return scenario;
            }
        }
        return SUCCESS;
    }
}
