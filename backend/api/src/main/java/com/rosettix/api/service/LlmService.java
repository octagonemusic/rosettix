package com.rosettix.api.service;

import org.springframework.stereotype.Service;

@Service
public class LlmService {

    public String generateQuery(String question) {
        // In the future, this method will call the Gemini API.
        // For now, we'll fake the response to test the complete flow.
        if (question.toLowerCase().contains("customers")) {
            return "SELECT * FROM customers;";
        }
        return "SELECT 1;"; // A safe default query
    }
}
