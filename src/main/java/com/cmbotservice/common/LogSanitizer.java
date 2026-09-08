package com.cmbotservice.common;

/**
 * Guardrail for the one place this service is tempted to log potentially sensitive
 * case-related free text (analyst prompts, ML Agent responses). {@link #preview}
 * returns a short, truncated preview intended for DEBUG-level logging only — callers
 * must never log full prompt/response content at INFO or above.
 */
public final class LogSanitizer {

    private static final int PREVIEW_LENGTH = 40;

    private LogSanitizer() {
    }

    public static String preview(String text) {
        if (text == null) {
            return "null";
        }
        String singleLine = text.replaceAll("\\s+", " ").trim();
        if (singleLine.length() <= PREVIEW_LENGTH) {
            return singleLine;
        }
        return singleLine.substring(0, PREVIEW_LENGTH) + "...(truncated, len=" + text.length() + ")";
    }
}
