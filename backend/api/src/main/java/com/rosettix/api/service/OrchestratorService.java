package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import com.rosettix.api.strategy.QueryStrategy;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrchestratorService {

    private final LlmService llmService;
    private final Map<String, QueryStrategy> strategies; // Spring will inject all strategies here
    private final RosettixConfiguration rosettixConfiguration;

    public List<Map<String, Object>> processQuery(String question) {
        // Use the configured default strategy
        return processQuery(
            question,
            rosettixConfiguration.getDefaultStrategy()
        );
    }

    public List<Map<String, Object>> processQuery(
        String question,
        String strategyName
    ) {
        QueryStrategy strategy = strategies.get(strategyName);

        if (strategy == null) {
            log.error("No strategy found for: {}", strategyName);
            throw new QueryException(
                "Unsupported database strategy: " + strategyName,
                strategyName,
                QueryException.ErrorType.STRATEGY_NOT_FOUND
            );
        }

        log.info(
            "Processing query with strategy: {}",
            strategy.getStrategyName()
        );

        String generatedQuery = llmService.generateQuery(question, strategy);

        if (!strategy.isQuerySafe(generatedQuery)) {
            log.warn("Generated query failed safety check: {}", generatedQuery);
            throw new QueryException(
                "Generated query is not safe to execute",
                strategyName,
                generatedQuery,
                QueryException.ErrorType.UNSAFE_QUERY
            );
        }

        try {
            log.info("Executing query: {}", generatedQuery);
            return strategy.executeQuery(generatedQuery);
        } catch (IllegalArgumentException e) {
            log.error(
                "Query parsing error with strategy {}: {}",
                strategyName,
                e.getMessage(),
                e
            );
            throw new QueryException(
                "Query parsing error: " + e.getMessage(),
                strategyName,
                generatedQuery,
                QueryException.ErrorType.PARSING_ERROR,
                e
            );
        } catch (com.mongodb.MongoException e) {
            log.error(
                "MongoDB database error with strategy {}: {}",
                strategyName,
                e.getMessage(),
                e
            );
            throw new QueryException(
                "Database connection error: " + e.getMessage(),
                strategyName,
                generatedQuery,
                QueryException.ErrorType.DATABASE_CONNECTION_ERROR,
                e
            );
        } catch (org.bson.json.JsonParseException e) {
            log.error(
                "JSON parsing error with strategy {}: {}",
                strategyName,
                e.getMessage(),
                e
            );
            throw new QueryException(
                "Query filter parsing error: " + e.getMessage(),
                strategyName,
                generatedQuery,
                QueryException.ErrorType.PARSING_ERROR,
                e
            );
        } catch (RuntimeException e) {
            // Check if it's a wrapped database/parsing error
            String message = e.getMessage();
            if (message.contains("MongoDB") || message.contains("Database")) {
                log.error(
                    "Runtime database error with strategy {}: {}",
                    strategyName,
                    e.getMessage(),
                    e
                );
                throw new QueryException(
                    e.getMessage(),
                    strategyName,
                    generatedQuery,
                    QueryException.ErrorType.DATABASE_CONNECTION_ERROR,
                    e
                );
            } else if (
                message.contains("parse") ||
                message.contains("Parse") ||
                message.contains("JSON")
            ) {
                log.error(
                    "Runtime parsing error with strategy {}: {}",
                    strategyName,
                    e.getMessage(),
                    e
                );
                throw new QueryException(
                    e.getMessage(),
                    strategyName,
                    generatedQuery,
                    QueryException.ErrorType.PARSING_ERROR,
                    e
                );
            } else {
                log.error(
                    "Runtime execution error with strategy {}: {}",
                    strategyName,
                    e.getMessage(),
                    e
                );
                throw new QueryException(
                    e.getMessage(),
                    strategyName,
                    generatedQuery,
                    QueryException.ErrorType.EXECUTION_ERROR,
                    e
                );
            }
        } catch (Exception e) {
            log.error(
                "Unexpected error executing query with strategy {}: {}",
                strategyName,
                e.getMessage(),
                e
            );
            throw new QueryException(
                "Unexpected execution error: " + e.getMessage(),
                strategyName,
                generatedQuery,
                QueryException.ErrorType.EXECUTION_ERROR,
                e
            );
        }
    }

    public Set<String> getAvailableStrategies() {
        return strategies.keySet();
    }
}
