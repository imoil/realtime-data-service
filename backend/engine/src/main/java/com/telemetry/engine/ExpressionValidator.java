package com.telemetry.engine;

import java.util.regex.Pattern;

/**
 * Security validation utility for expression strings before AviatorScript compilation.
 * Blocks code injection, classloader access, reflection, loops, and unauthorized system calls.
 */
public final class ExpressionValidator {

    private static final int MAX_EXPRESSION_LENGTH = 500;

    private static final Pattern ALLOWED_CHARACTERS_PATTERN = Pattern.compile(
            "^[a-zA-Z0-9_\\s\\+\\-\\*/\\%\\?\\:\\>\\<\\=\\!\\&\\|\\(\\)\\,\\.]+$"
    );

    private static final Pattern ASSIGNMENT_OPERATOR_PATTERN = Pattern.compile(
            "(?<![=><!])=(?![=])"
    );

    private static final Pattern DISALLOWED_KEYWORDS_PATTERN = Pattern.compile(
            "(?i)\\b(let|fn|def|while|for|in|return|try|catch|throw|use|import|include|load|new|Class|System|Runtime|Process|ProcessBuilder|exec|eval|sys|system|java|javax|org|com|net|lambda|nil|null)\\b"
    );

    private ExpressionValidator() {
    }

    /**
     * Validates safety of expression text before compilation.
     *
     * @param expressionText input formula string
     * @throws IllegalArgumentException if formula is null, empty, exceeds limit, or violates security rules
     */
    public static void validate(String expressionText) {
        if (expressionText == null || expressionText.trim().isEmpty()) {
            throw new IllegalArgumentException("Expression text must not be null or empty.");
        }

        if (expressionText.length() > MAX_EXPRESSION_LENGTH) {
            throw new IllegalArgumentException("Expression text exceeds maximum allowed length of " + MAX_EXPRESSION_LENGTH + " characters.");
        }

        if (!ALLOWED_CHARACTERS_PATTERN.matcher(expressionText).matches()) {
            throw new IllegalArgumentException("Expression contains invalid or unsafe characters.");
        }

        if (DISALLOWED_KEYWORDS_PATTERN.matcher(expressionText).find()) {
            throw new IllegalArgumentException("Expression contains disallowed keywords.");
        }

        if (ASSIGNMENT_OPERATOR_PATTERN.matcher(expressionText).find()) {
            throw new IllegalArgumentException("Assignment operations are not allowed in expressions.");
        }
    }
}
