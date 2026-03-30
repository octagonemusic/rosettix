package com.rosettix.api.service;

import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class LlmService {

    private static final java.util.regex.Pattern RETRY_AFTER_PATTERN =
        java.util.regex.Pattern.compile("retry in\\s+([0-9]+(?:\\.[0-9]+)?)s", java.util.regex.Pattern.CASE_INSENSITIVE);

    private final Client geminiClient; // Injected from your GeminiConfig
    private final RosettixConfiguration rosettixConfiguration;

    public String generateQuery(String question, QueryStrategy strategy) {
        String schema = strategy.getSchemaRepresentation();
        String prompt = strategy.buildPrompt(question, schema);

        try {
            String modelName = rosettixConfiguration.getLlm().getModelName();
            GenerateContentResponse response =
                geminiClient.models.generateContent(modelName, prompt, null);

            // Clean the response using strategy-specific cleaning
            return strategy.cleanQuery(response.text());
        } catch (Exception e) {
            log.error(
                "Error calling Gemini API for question '{}': {}",
                question,
                e.getMessage(),
                e
            );

            if (isRateLimited(e)) {
                throw new QueryException(
                    buildRateLimitMessage(e),
                    strategy.getStrategyName(),
                    null,
                    QueryException.ErrorType.RATE_LIMITED,
                    e
                );
            }

            throw new QueryException(
                "Error calling Gemini API: " + e.getMessage(),
                strategy.getStrategyName(),
                null,
                QueryException.ErrorType.LLM_ERROR,
                e
            );
        }
    }

    private boolean isRateLimited(Exception e) {
        String message = e.getMessage();
        if (message == null) {
            return false;
        }

        String lower = message.toLowerCase();
        return lower.contains("429")
            || lower.contains("too many requests")
            || lower.contains("quota exceeded")
            || lower.contains("rate limit");
    }

    private String buildRateLimitMessage(Exception e) {
        String retryAfter = extractRetryAfterSeconds(e.getMessage());
        if (retryAfter != null) {
            return "Gemini rate limit reached. Wait about " + retryAfter + " seconds and try again, or switch to a paid API key/project.";
        }
        return "Gemini rate limit reached. Please retry shortly, or switch to a paid API key/project.";
    }

    private String extractRetryAfterSeconds(String message) {
        if (message == null) {
            return null;
        }

        java.util.regex.Matcher matcher = RETRY_AFTER_PATTERN.matcher(message);
        if (!matcher.find()) {
            return null;
        }

        String raw = matcher.group(1);
        try {
            double seconds = Double.parseDouble(raw);
            return String.valueOf((int) Math.ceil(seconds));
        } catch (NumberFormatException ignored) {
            return raw;
        }
    }
}
