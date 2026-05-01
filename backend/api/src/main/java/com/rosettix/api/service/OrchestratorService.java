package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import com.rosettix.api.saga.Saga;
import com.rosettix.api.saga.SagaOrchestrator;
import com.rosettix.api.saga.SagaStep;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrchestratorService {

    private final LlmService llmService;
    private final QueryGenerationCacheService queryGenerationCacheService;
    private final Map<String, QueryStrategy> strategies; // Spring auto-injects all @Component strategies
    private final RosettixConfiguration rosettixConfiguration;
    @Autowired
    private SagaOrchestrator sagaOrchestrator;

    // ============================================================
    // 1️⃣ READ-ONLY QUERIES (Handled by /api/query)
    // ============================================================
    public List<Map<String, Object>> processQuery(String question) {
        return processQuery(question, rosettixConfiguration.getDefaultStrategy());
    }

    public List<Map<String, Object>> processQuery(String question, String strategyName) {
        QueryStrategy strategy = strategies.get(strategyName);

        if (strategy == null) {
            log.error("No strategy found for: {}", strategyName);
            throw new QueryException(
                    "Unsupported database strategy: " + strategyName,
                    strategyName,
                    QueryException.ErrorType.STRATEGY_NOT_FOUND
            );
        }

        log.info("Processing READ query with strategy: {}", strategy.getStrategyName());

        String schemaRepresentation = strategy.getSchemaRepresentation();
        String cachedQuery = queryGenerationCacheService.getCachedQuery(question, strategy, schemaRepresentation);
        boolean cacheHit = cachedQuery != null;

        // 🔹 Generate and clean query
        String generatedQuery = cacheHit
                ? cachedQuery
                : llmService.generateQuery(question, strategy, schemaRepresentation);
        String cleanedQuery = strategy.cleanQuery(generatedQuery);

        // 🔹 Validate basic query safety
        if (!strategy.isQuerySafe(cleanedQuery)) {
            log.warn("Generated query failed safety check: {}", cleanedQuery);
            throw new QueryException(
                    "Generated query is not safe to execute",
                    strategyName,
                    cleanedQuery,
                    QueryException.ErrorType.UNSAFE_QUERY
            );
        }

        // 🔒 Strict read-only enforcement
        if (!strategy.isReadOperation(cleanedQuery)) {
            log.warn("❌ Blocked non-read query on /api/query: {}", cleanedQuery);
            throw new QueryException(
                    "Only read operations (SELECT/find/count) are allowed on this endpoint. " +
                    "Use /api/query/write for insert, update, or delete queries.",
                    strategyName,
                    cleanedQuery,
                    QueryException.ErrorType.UNSUPPORTED_OPERATION
            );
        }

        try {
            log.info("Executing read query: {}", cleanedQuery);
            List<Map<String, Object>> results = strategy.executeQuery(cleanedQuery);
            if (!cacheHit) {
                queryGenerationCacheService.cacheQuery(question, strategy, schemaRepresentation, cleanedQuery);
            }
            return results;
        } catch (RuntimeException e) {
            handleRuntimeError(e, strategyName, cleanedQuery);
            return List.of();
        } catch (Exception e) {
            log.error("Unexpected error executing query with strategy {}: {}", strategyName, e.getMessage(), e);
            throw new QueryException(
                    "Unexpected execution error: " + e.getMessage(),
                    strategyName,
                    cleanedQuery,
                    QueryException.ErrorType.EXECUTION_ERROR,
                    e
            );
        }
    }

    // ============================================================
    // 2️⃣ WRITE QUERIES (Handled by /api/query/write)
    // ============================================================
    public List<Map<String, Object>> processWriteQuery(String question, String strategyName) {
        QueryStrategy strategy = strategies.get(strategyName);

        if (strategy == null) {
            log.error("No strategy found for: {}", strategyName);
            throw new QueryException(
                    "Unsupported database strategy: " + strategyName,
                    strategyName,
                    QueryException.ErrorType.STRATEGY_NOT_FOUND
            );
        }

        log.info("Processing WRITE query with strategy: {}", strategy.getStrategyName());

        try {
            // 🔹 Use same generation pipeline for consistency
            String generatedQuery = llmService.generateQuery(question, strategy);
            String cleanedQuery = strategy.cleanQuery(generatedQuery);

            log.info("Generated write query: {}", cleanedQuery);

            // 🔹 Safety validation
            if (!strategy.isQuerySafe(cleanedQuery)) {
                throw new QueryException(
                        "Unsafe write query detected: " + cleanedQuery,
                        strategyName,
                        cleanedQuery,
                        QueryException.ErrorType.UNSAFE_QUERY
                );
            }

            // 🔒 Only allow write operations
            if (!strategy.isWriteOperation(cleanedQuery)) {
                log.warn("❌ Blocked non-write query on /api/query/write: {}", cleanedQuery);
                throw new QueryException(
                        "Only INSERT, UPDATE, or DELETE operations are allowed on this endpoint. " +
                        "Use /api/query for read operations.",
                        strategyName,
                        cleanedQuery,
                        QueryException.ErrorType.UNSUPPORTED_OPERATION
                );
            }

            // ✅ Execute the write query safely
            return strategy.executeQuery(cleanedQuery);

        } catch (RuntimeException e) {
            handleRuntimeError(e, strategyName, question);
            return List.of();
        } catch (Exception e) {
            log.error("Unexpected error executing write query: {}", e.getMessage(), e);
            throw new QueryException(
                    "Unexpected execution error: " + e.getMessage(),
                    strategyName,
                    question,
                    QueryException.ErrorType.EXECUTION_ERROR,
                    e
            );
        }
    }

    // ============================================================
    // 3️⃣ CENTRALIZED RUNTIME ERROR HANDLER
    // ============================================================
    private void handleRuntimeError(RuntimeException e, String strategyName, String query) {
        String message = e.getMessage();
        if (message == null) message = "Unknown runtime error";

        if (message.contains("MongoDB") || message.contains("Database")) {
            throw new QueryException(
                    "Database connection error: " + message,
                    strategyName,
                    query,
                    QueryException.ErrorType.DATABASE_CONNECTION_ERROR,
                    e
            );
        } else if (message.contains("parse") || message.contains("JSON")) {
            throw new QueryException(
                    "Query parsing error: " + message,
                    strategyName,
                    query,
                    QueryException.ErrorType.PARSING_ERROR,
                    e
            );
        } else {
            throw new QueryException(
                    "Execution error: " + message,
                    strategyName,
                    query,
                    QueryException.ErrorType.EXECUTION_ERROR,
                    e
            );
        }
    }

    // ============================================================
    // 4️⃣ STRATEGY ENUMERATION
    // ============================================================
    public Set<String> getAvailableStrategies() {
        return strategies.keySet();
    }

    public List<Map<String, Object>> processSaga(List<SagaStep> steps, boolean isWrite) {
        Saga saga = new Saga();
        steps.forEach(saga::addStep);
        return sagaOrchestrator.executeSaga(saga, isWrite);
    }
}
