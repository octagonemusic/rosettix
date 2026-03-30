package com.rosettix.api.saga;

import com.rosettix.api.exception.QueryException;
import com.rosettix.api.service.LlmService;
import com.rosettix.api.strategy.CassandraStrategy;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Executes multi-step transactions using the Saga pattern.
 * Rolls back prior write steps if any later write step fails.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaOrchestrator {

    private final Map<String, QueryStrategy> strategies;
    private final LlmService llmService;

    public List<Map<String, Object>> executeSaga(Saga saga, boolean isWrite) {
        log.info("Starting Saga ID={} with {} steps (write={})", saga.getSagaId(), saga.getSteps().size(), isWrite);
        List<Map<String, Object>> allResults = new ArrayList<>();
        Deque<SagaStep> executedSteps = new ArrayDeque<>();

        for (SagaStep step : saga.getSteps()) {
            String strategyName = step.getDatabase().toLowerCase(Locale.ROOT);
            QueryStrategy strategy = strategies.get(strategyName);

            if (strategy == null) {
                throw new QueryException(
                        "Unknown strategy: " + strategyName,
                        strategyName,
                        QueryException.ErrorType.STRATEGY_NOT_FOUND
                );
            }

            try {
                String forwardQuery = llmService.generateQuery(step.getQuestion(), strategy);
                forwardQuery = strategy.cleanQuery(forwardQuery);
                forwardQuery = adaptWriteQueryIfNeeded(step.getQuestion(), strategy, forwardQuery, isWrite);
                step.setForwardQuery(forwardQuery);

                log.info("Executing step (DB={}): {}", strategyName, forwardQuery);
                if (!strategy.isQuerySafe(forwardQuery)) {
                    throw new QueryException(
                            "Unsafe query detected",
                            strategyName,
                            forwardQuery,
                            QueryException.ErrorType.UNSAFE_QUERY
                    );
                }

                validateQueryMode(forwardQuery, strategyName, isWrite);

                List<Map<String, Object>> result = strategy.executeQuery(forwardQuery);
                allResults.addAll(result);
                executedSteps.push(step);

                String compensationQuery = null;
                if (isWrite) {
                    String compensationPrompt =
                            "Generate the compensation (rollback) query for reversing this operation:\n" +
                            forwardQuery + "\n" +
                            "Only return a rollback query if it can be generated safely using exact values from the forward query.\n" +
                            "If the rollback cannot be determined safely, return exactly UNSUPPORTED.";

                    compensationQuery = llmService.generateQuery(compensationPrompt, strategy);
                    compensationQuery = strategy.cleanQuery(compensationQuery);

                    if ("UNSUPPORTED".equalsIgnoreCase(compensationQuery)) {
                        compensationQuery = null;
                    }
                }

                step.setCompensationQuery(compensationQuery);
                log.info("Step succeeded for DB={} | Compensation Prepared: {}", strategyName, compensationQuery);

            } catch (Exception e) {
                log.error("Step failed on DB={} | Reason: {}", step.getDatabase(), e.getMessage());
                if (isWrite) {
                    performRollback(executedSteps);
                }
                throw new QueryException(
                        "Saga failed and rolled back: " + e.getMessage(),
                        strategyName,
                        QueryException.ErrorType.EXECUTION_ERROR,
                        e
                );
            }
        }

        log.info("Saga ID={} completed successfully", saga.getSagaId());
        return allResults;
    }

    private void performRollback(Deque<SagaStep> executedSteps) {
        log.warn("Performing rollback for {} completed steps...", executedSteps.size());
        while (!executedSteps.isEmpty()) {
            SagaStep step = executedSteps.pop();
            try {
                String strategyName = step.getDatabase().toLowerCase(Locale.ROOT);
                QueryStrategy strategy = strategies.get(strategyName);
                String rollbackQuery = step.getCompensationQuery();

                if (rollbackQuery == null || rollbackQuery.isBlank()) {
                    log.warn("No rollback query found for step in {}", strategyName);
                    continue;
                }

                log.info("Executing rollback on {}: {}", strategyName, rollbackQuery);
                strategy.executeQuery(rollbackQuery);

            } catch (Exception ex) {
                log.error("Rollback failed for DB={} | Reason: {}", step.getDatabase(), ex.getMessage());
            }
        }
        log.warn("Rollback completed for saga.");
    }

    private void validateQueryMode(String query, String strategyName, boolean isWrite) {
        String lower = query.toLowerCase(Locale.ROOT);

        boolean readQuery = lower.startsWith("select")
                || lower.contains(".find(")
                || lower.contains(".count(");

        boolean writeQuery = lower.startsWith("insert")
                || lower.startsWith("update")
                || lower.startsWith("delete")
                || lower.contains(".insertone(")
                || lower.contains(".updateone(")
                || lower.contains(".deleteone(");

        if (isWrite && !writeQuery) {
            throw new QueryException(
                    "Write saga received a non-write query",
                    strategyName,
                    query,
                    QueryException.ErrorType.UNSUPPORTED_OPERATION
            );
        }

        if (!isWrite && !readQuery) {
            throw new QueryException(
                    "Read saga received a non-read query",
                    strategyName,
                    query,
                    QueryException.ErrorType.UNSUPPORTED_OPERATION
            );
        }
    }

    private String adaptWriteQueryIfNeeded(String question, QueryStrategy strategy, String query, boolean isWrite) {
        if (isWrite && strategy instanceof CassandraStrategy cassandraStrategy) {
            return cassandraStrategy.adaptWriteQuery(question, query);
        }
        return query;
    }
}
