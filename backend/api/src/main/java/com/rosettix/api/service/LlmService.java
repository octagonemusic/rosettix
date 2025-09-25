package com.rosettix.api.service;

import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import com.rosettix.api.service.SchemaManagerService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LlmService {

    private final Client geminiClient; // Injected from your GeminiConfig
    private final SchemaManagerService schemaManagerService;

    private static final String MODEL_NAME = "gemini-2.5-flash";

    public String generateQuery(String question) {
        String schema = schemaManagerService.getDatabaseSchema();
        String prompt = String.format(
            "Given the SQL schema: %s\n---\nTranslate the following question into a single, valid PostgreSQL query. Do not add any explanation, comments, or markdown formatting.\nQuestion: \"%s\"",
            schema,
            question
        );

        try {
            GenerateContentResponse response =
                geminiClient.models.generateContent(MODEL_NAME, prompt, null);

            return response.text().trim();
        } catch (Exception e) {
            // Catching the general Exception class will handle any error from the SDK
            System.err.println("Error calling Gemini API: " + e.getMessage());
            e.printStackTrace();
            return "Error: Could not generate query.";
        }
    }
}
