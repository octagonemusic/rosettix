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

    private final Client geminiClient; // Injected from your GeminiConfig
    private final RosettixConfiguration rosettixConfiguration;

    public String generateQuery(String question, QueryStrategy strategy) {
        String schema = strategy.getSchemaRepresentation();
        String queryLanguage = strategy.getQueryLanguage();

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
            throw new QueryException(
                "Error calling Gemini API: " + e.getMessage(),
                strategy.getStrategyName(),
                null,
                QueryException.ErrorType.LLM_ERROR,
                e
            );
        }
    }
}