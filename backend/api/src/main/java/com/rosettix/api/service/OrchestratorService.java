package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import com.rosettix.api.saga.Saga;
import com.rosettix.api.saga.SagaOrchestrator;
import com.rosettix.api.saga.SagaStep;
import com.rosettix.api.strategy.CassandraStrategy;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrchestratorService {

    private final LlmService llmService;
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
        String normalizedStrategyName = normalizeStrategyName(strategyName);
        QueryStrategy strategy = strategies.get(normalizedStrategyName);

        if (strategy == null) {
            log.error("No strategy found for: {}", strategyName);
            throw new QueryException(
                    "Unsupported database strategy: " + strategyName,
                    normalizedStrategyName,
                    QueryException.ErrorType.STRATEGY_NOT_FOUND
            );
        }

        log.info("Processing READ query with strategy: {}", strategy.getStrategyName());

        // 🔹 Generate and clean query
        String generatedQuery = llmService.generateQuery(question, strategy);
        String cleanedQuery = strategy.cleanQuery(generatedQuery);

        // 🔹 Validate basic query safety
        if (!strategy.isQuerySafe(cleanedQuery)) {
            log.warn("Generated query failed safety check: {}", cleanedQuery);
            throw new QueryException(
                    "Generated query is not safe to execute",
                    normalizedStrategyName,
                    cleanedQuery,
                    QueryException.ErrorType.UNSAFE_QUERY
            );
        }

        // 🔒 Strict read-only enforcement
        String lower = cleanedQuery.toLowerCase();
        boolean isReadQuery =
                lower.startsWith("select") ||
                lower.contains(".find(") ||
                lower.contains(".count(");

        if (!isReadQuery) {
            log.warn("❌ Blocked non-read query on /api/query: {}", cleanedQuery);
            throw new QueryException(
                    "Only read operations (SELECT/find/count) are allowed on this endpoint. " +
                    "Use /api/query/write for insert, update, or delete queries.",
                    normalizedStrategyName,
                    cleanedQuery,
                    QueryException.ErrorType.UNSUPPORTED_OPERATION
            );
        }

        try {
            log.info("Executing read query: {}", cleanedQuery);
            return attachExecutedQueryMetadata(strategy.executeQuery(cleanedQuery), cleanedQuery);
        } catch (RuntimeException e) {
            handleRuntimeError(e, normalizedStrategyName, cleanedQuery);
            return List.of();
        } catch (Exception e) {
            log.error("Unexpected error executing query with strategy {}: {}", normalizedStrategyName, e.getMessage(), e);
            throw new QueryException(
                    "Unexpected execution error: " + e.getMessage(),
                    normalizedStrategyName,
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
        String normalizedStrategyName = normalizeStrategyName(strategyName);
        QueryStrategy strategy = strategies.get(normalizedStrategyName);

        if (strategy == null) {
            log.error("No strategy found for: {}", strategyName);
            throw new QueryException(
                    "Unsupported database strategy: " + strategyName,
                    normalizedStrategyName,
                    QueryException.ErrorType.STRATEGY_NOT_FOUND
            );
        }

        log.info("Processing WRITE query with strategy: {}", strategy.getStrategyName());

        try {
            // 🔹 Use same generation pipeline for consistency
            String generatedQuery = llmService.generateQuery(question, strategy);
            String cleanedQuery = strategy.cleanQuery(generatedQuery);
            cleanedQuery = adaptWriteQueryIfNeeded(question, strategy, cleanedQuery);

            log.info("Generated write query: {}", cleanedQuery);

            // 🔹 Safety validation
            if (!strategy.isQuerySafe(cleanedQuery)) {
                throw new QueryException(
                        "Unsafe write query detected: " + cleanedQuery,
                        normalizedStrategyName,
                        cleanedQuery,
                        QueryException.ErrorType.UNSAFE_QUERY
                );
            }

            // 🔒 Only allow write operations
            String lower = cleanedQuery.toLowerCase();
            boolean isWriteQuery =
                    lower.contains("insert") ||
                    lower.contains("update") ||
                    lower.contains("delete") ||
                    lower.contains(".insertone(") ||
                    lower.contains(".updateone(") ||
                    lower.contains(".deleteone(");

            if (!isWriteQuery) {
                log.warn("❌ Blocked non-write query on /api/query/write: {}", cleanedQuery);
                throw new QueryException(
                        "Only INSERT, UPDATE, or DELETE operations are allowed on this endpoint. " +
                        "Use /api/query for read operations.",
                        normalizedStrategyName,
                        cleanedQuery,
                        QueryException.ErrorType.UNSUPPORTED_OPERATION
                );
            }

            // ✅ Execute the write query safely
            return attachExecutedQueryMetadata(strategy.executeQuery(cleanedQuery), cleanedQuery);

        } catch (RuntimeException e) {
            handleRuntimeError(e, normalizedStrategyName, question);
            return List.of();
        } catch (Exception e) {
            log.error("Unexpected error executing write query: {}", e.getMessage(), e);
            throw new QueryException(
                    "Unexpected execution error: " + e.getMessage(),
                    normalizedStrategyName,
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

    private String normalizeStrategyName(String strategyName) {
        if (strategyName == null) {
            return rosettixConfiguration.getDefaultStrategy().toLowerCase(Locale.ROOT);
        }
        return strategyName.trim().toLowerCase(Locale.ROOT);
    }

    private String adaptWriteQueryIfNeeded(String question, QueryStrategy strategy, String query) {
        if (strategy instanceof CassandraStrategy cassandraStrategy) {
            return cassandraStrategy.adaptWriteQuery(question, query);
        }
        return query;
    }

    private List<Map<String, Object>> attachExecutedQueryMetadata(List<Map<String, Object>> results, String executedQuery) {
        List<Map<String, Object>> normalizedResults = results == null ? List.of() : results;
        if (normalizedResults.isEmpty()) {
            return List.of(Map.of("_executed_query", executedQuery));
        }

        return normalizedResults.stream()
                .map(row -> {
                    java.util.LinkedHashMap<String, Object> copy = new java.util.LinkedHashMap<>(row);
                    copy.put("_executed_query", executedQuery);
                    return copy;
                })
                .map(row -> (Map<String, Object>) row)
                .toList();
    }
}
