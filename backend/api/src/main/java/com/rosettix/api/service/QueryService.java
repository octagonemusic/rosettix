package com.rosettix.api.service;

import com.rosettix.api.exception.QueryException;
import com.rosettix.api.saga.*;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Unified query service that supports:
 * - Single read/write queries
 * - Multi-step distributed Saga transactions
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class QueryService {

    private final Map<String, QueryStrategy> strategyMap;   // Injected strategies
    private final LlmService llmService;                    // For query + compensation generation
    private final SagaOrchestrator sagaOrchestrator;        // Handles saga execution and rollback

    // ============================================================
    // 1️⃣ PROCESS SINGLE READ/WRITE QUERY
    // ============================================================
    public List<Map<String, Object>> processQuery(String question, String dbType) {
        QueryStrategy strategy = strategyMap.get(dbType.toLowerCase());
        if (strategy == null)
            throw new QueryException("Unsupported database type: " + dbType, dbType, QueryException.ErrorType.STRATEGY_NOT_FOUND);

        log.info("Using strategy: {}", strategy.getStrategyName());

        // Build schema-aware LLM prompt
        String schema = strategy.getSchemaRepresentation();
        String generatedQuery = llmService.generateQuery(question, strategy);
        String cleanedQuery = strategy.cleanQuery(generatedQuery);

        log.info("Generated query: {}", cleanedQuery);

        // Detect write operations
        boolean isWrite = isWriteQuery(cleanedQuery);

        try {
            if (isWrite) {
                log.info("Detected WRITE query → executing under Saga pattern.");

                Saga saga = new Saga();
                SagaStep step = new SagaStep(
                        question,
                        dbType,
                        cleanedQuery,
                        null
                );
                saga.addStep(step);

                return sagaOrchestrator.executeSaga(saga, true);
            } else {
                log.info("Detected READ query → executing directly.");
                return strategy.executeQuery(cleanedQuery);
            }
        } catch (Exception e) {
            log.error("Error executing query: {}", e.getMessage(), e);
            throw new QueryException(
                    "Query execution failed: " + e.getMessage(),
                    dbType,
                    cleanedQuery,
                    QueryException.ErrorType.EXECUTION_ERROR,
                    e
            );
        }
    }

    // ============================================================
    // 2️⃣ PROCESS MULTI-STEP SAGA TRANSACTION
    // ============================================================
    public List<Map<String, Object>> processSaga(List<SagaStep> steps, boolean isWrite) {
        log.info("Starting Saga with {} steps (write={})", steps.size(), isWrite);
        Saga saga = new Saga();
        steps.forEach(saga::addStep);
        return sagaOrchestrator.executeSaga(saga, isWrite);
    }

    // ============================================================
    // 3️⃣ HELPER: DETECT WRITE OPERATIONS
    // ============================================================
    private boolean isWriteQuery(String query) {
        if (query == null) return false;
        String lower = query.toLowerCase(Locale.ROOT);
        return lower.startsWith("insert") ||
               lower.startsWith("update") ||
               lower.startsWith("delete") ||
               lower.contains(".insertone(") ||
               lower.contains(".updateone(") ||
               lower.contains(".deleteone(");
    }
}