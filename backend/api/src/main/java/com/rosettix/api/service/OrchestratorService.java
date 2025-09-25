package com.rosettix.api.service;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class OrchestratorService {

    private final LlmService llmService;

    private final DbExecutionService dbExecutionService;

    public List<Map<String, Object>> processQuery(String question) {
        // Step 1: Get the SQL from the LLM service
        String sqlQuery = llmService.generateQuery(question);

        // Step 2: Execute the SQL and return the result
        return dbExecutionService.executeSql(sqlQuery);
    }
}
