package com.rosettix.api.saga;

import com.rosettix.api.exception.QueryException;
import com.rosettix.api.service.LlmService;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * Executes multi-step transactions using the Saga pattern.
 * Rolls back all prior steps if any step fails.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class SagaOrchestrator {

    private final Map<String, QueryStrategy> strategies;
    private final LlmService llmService;

    /**
     * Executes all saga steps sequentially.
     */
    public List<Map<String, Object>> executeSaga(Saga saga, boolean isWrite) {
        log.info("🚀 Starting Saga ID={} with {} steps (write={})", saga.getSagaId(), saga.getSteps().size(), isWrite);
        List<Map<String, Object>> allResults = new ArrayList<>();
        Deque<SagaStep> executedSteps = new ArrayDeque<>();

        for (SagaStep step : saga.getSteps()) {
            String strategyName = step.getDatabase().toLowerCase();
            QueryStrategy strategy = strategies.get(strategyName);

            if (strategy == null) {
                throw new QueryException("Unknown strategy: " + strategyName, strategyName, QueryException.ErrorType.STRATEGY_NOT_FOUND);
            }

            try {
                // Step 1️⃣: Generate forward query
                String forwardQuery = llmService.generateQuery(step.getQuestion(), strategy);
                forwardQuery = strategy.cleanQuery(forwardQuery);
                step.setForwardQuery(forwardQuery);

                log.info("Executing step (DB={}): {}", strategyName, forwardQuery);
                if (!strategy.isQuerySafe(forwardQuery)) {
                    throw new QueryException("Unsafe query detected", strategyName, forwardQuery, QueryException.ErrorType.UNSAFE_QUERY);
                }

                List<Map<String, Object>> result = strategy.executeQuery(forwardQuery);
                allResults.addAll(result);
                executedSteps.push(step); // ✅ Mark as completed

                // Step 2️⃣: Generate compensation query for rollback
                String compensationPrompt = "Generate the compensation (rollback) query for reversing this operation:\n" +
                        forwardQuery + "\nReturn only the query.";
                String compensationQuery = llmService.generateQuery(compensationPrompt, strategy);
                compensationQuery = strategy.cleanQuery(compensationQuery);
                step.setCompensationQuery(compensationQuery);

                log.info("✅ Step succeeded for DB={} | Compensation Prepared: {}", strategyName, compensationQuery);

            } catch (Exception e) {
                log.error("❌ Step failed on DB={} | Reason: {}", step.getDatabase(), e.getMessage());
                performRollback(executedSteps);
                throw new QueryException("Saga failed and rolled back: " + e.getMessage(), strategyName, QueryException.ErrorType.EXECUTION_ERROR, e);
            }
        }

        log.info("🎯 Saga ID={} completed successfully", saga.getSagaId());
        return allResults;
    }

    /**
     * Rolls back all previously executed steps in reverse order.
     */
    private void performRollback(Deque<SagaStep> executedSteps) {
        log.warn("⚠️ Performing rollback for {} completed steps...", executedSteps.size());
        while (!executedSteps.isEmpty()) {
            SagaStep step = executedSteps.pop();
            try {
                String strategyName = step.getDatabase().toLowerCase();
                QueryStrategy strategy = strategies.get(strategyName);
                String rollbackQuery = step.getCompensationQuery();

                if (rollbackQuery == null || rollbackQuery.isBlank()) {
                    log.warn("⚠️ No rollback query found for step in {}", strategyName);
                    continue;
                }

                log.info("🔄 Executing rollback on {}: {}", strategyName, rollbackQuery);
                strategy.executeQuery(rollbackQuery);

            } catch (Exception ex) {
                log.error("⚠️ Rollback failed for DB={} | Reason: {}", step.getDatabase(), ex.getMessage());
            }
        }
        log.warn("🧹 Rollback completed for saga.");
    }
}