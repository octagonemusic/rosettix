package com.rosettix.api.service;

import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LlmService {

    private final Client geminiClient; // Injected from your GeminiConfig

    private static final String MODEL_NAME = "gemini-2.5-flash";

    public String generateQuery(String question) {
        String schema =
            "CREATE TABLE customers (id SERIAL PRIMARY KEY, name VARCHAR(255), city VARCHAR(100));";
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
