package com.rosettix.api.service;

import com.rosettix.api.strategy.DatabaseStrategy;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrchestratorService {

    private final LlmService llmService;
    private final Map<String, DatabaseStrategy> strategies; // Spring will inject all strategies here

    public List<Map<String, Object>> processQuery(String question) {
        // For now, we'll hardcode to use the "postgres" strategy.
        // Later, the user's request will specify which one to use.
        DatabaseStrategy strategy = strategies.get("postgres");

        String sqlQuery = llmService.generateQuery(question, strategy);
        return strategy.executeQuery(sqlQuery);
    }
}
