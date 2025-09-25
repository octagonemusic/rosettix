package com.rosettix.api.service;

import com.google.genai.Client;
import com.google.genai.types.GenerateContentResponse;
import com.rosettix.api.strategy.DatabaseStrategy;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class LlmService {

    private final Client geminiClient; // Injected from your GeminiConfig

    private static final String MODEL_NAME = "gemini-2.5-flash"; // Using a more recent model name

    public String generateQuery(String question, DatabaseStrategy strategy) {
        String schema = strategy.getSchemaRepresentation();
        String dialect = strategy.getQueryDialect();

        String prompt = String.format(
            "Given the %s schema: \n%s\n---\nTranslate the question into a single, valid %s query. Do not add any explanation, comments, or markdown formatting.\nQuestion: \"%s\"",
            dialect,
            schema,
            dialect,
            question
        );

        try {
            GenerateContentResponse response =
                geminiClient.models.generateContent(MODEL_NAME, prompt, null);

            // The KEY CHANGE is here: Clean the response before returning it.
            return cleanSqlQuery(response.text());
        } catch (Exception e) {
            System.err.println("Error calling Gemini API: " + e.getMessage());
            e.printStackTrace();
            return "Error: Could not generate query.";
        }
    }

    /**
     * Cleans the raw text response from the LLM by removing Markdown code blocks.
     * @param rawSql The raw text from the LLM.
     * @return A clean, executable SQL query.
     */
    private String cleanSqlQuery(String rawSql) {
        // Remove the markdown code block delimiters "```sql" or "```"
        String cleanedSql = rawSql.replace("```sql", "").replace("```", "");
        // Trim any leading/trailing whitespace and semicolons
        return cleanedSql.trim().replaceAll(";", "");
    }
}
