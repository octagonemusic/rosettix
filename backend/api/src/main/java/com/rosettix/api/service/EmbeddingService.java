package com.rosettix.api.service;

import com.google.genai.Client;
import com.google.genai.types.ContentEmbedding;
import com.google.genai.types.EmbedContentConfig;
import com.google.genai.types.EmbedContentResponse;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class EmbeddingService {

    private final Client geminiClient;
    private final RosettixConfiguration rosettixConfiguration;

    public List<Float> embedText(String text) {
        try {
            String modelName = rosettixConfiguration.getLlm().getEmbeddingModelName();
            EmbedContentConfig embedConfig = EmbedContentConfig.builder()
                    .outputDimensionality(rosettixConfiguration.getLlm().getEmbeddingDimensions())
                    .build();
            EmbedContentResponse response = geminiClient.models.embedContent(modelName, text, embedConfig);
            List<ContentEmbedding> embeddings = response.embeddings().orElse(List.of());
            if (embeddings.isEmpty()) {
                throw new IllegalStateException("Embedding response did not contain any vectors");
            }

            List<Float> values = embeddings.get(0).values().orElse(List.of());
            if (values.isEmpty()) {
                throw new IllegalStateException("Embedding vector was empty");
            }

            return values;
        } catch (Exception e) {
            log.error("Error generating embeddings for query text '{}': {}", text, e.getMessage(), e);
            throw new QueryException(
                    "Error generating embeddings: " + e.getMessage(),
                    null,
                    null,
                    QueryException.ErrorType.LLM_ERROR,
                    e
            );
        }
    }
}
