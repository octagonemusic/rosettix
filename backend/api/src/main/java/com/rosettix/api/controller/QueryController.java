package com.rosettix.api.controller;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.dto.QueryRequest;
import com.rosettix.api.exception.QueryException;
import com.rosettix.api.saga.SagaStep;
import com.rosettix.api.service.OrchestratorService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
@Slf4j
public class QueryController {

    private final OrchestratorService orchestratorService;
    private final RosettixConfiguration rosettixConfiguration;

    // ============================================================
    // 1️⃣ READ-ONLY ENDPOINT (Supports Single Query or Saga)
    // ============================================================
    @PostMapping
    public ResponseEntity<?> handleQuery(@Valid @RequestBody Map<String, Object> requestBody) {
        try {
            // Check for Saga-style input
            if (requestBody.containsKey("steps")) {
                log.info("🌀 Saga READ request received.");

                List<Map<String, Object>> stepMaps = extractStepMaps(requestBody.get("steps"));

                List<SagaStep> steps = stepMaps.stream()
                        .map(m -> new SagaStep(
                                (String) m.get("question"),
                                (String) m.get("database"),
                                null,
                                null
                        ))
                        .collect(Collectors.toList());

                List<Map<String, Object>> result = orchestratorService.processSaga(steps, false);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("mode", "saga-read");
                response.put("timestamp", Instant.now().toString());
                response.put("saga_step_count", steps.size());
                response.put("executed_queries", steps.stream()
                        .map(SagaStep::getForwardQuery)
                        .filter(Objects::nonNull)
                        .toList());
                response.put("results", stripExecutionMetadata(result));
                return ResponseEntity.ok(response);
            }

            // ✅ Legacy single-query support
            QueryRequest request = new QueryRequest();
            request.setQuestion((String) requestBody.get("question"));
            request.setDatabase((String) requestBody.get("database"));

            String strategy = request.getDatabase() != null
                    ? request.getDatabase().toLowerCase()
                    : rosettixConfiguration.getDefaultStrategy();

            List<Map<String, Object>> result =
                    orchestratorService.processQuery(request.getQuestion(), strategy);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("mode", "single-read");
            response.put("strategy", strategy);
            response.put("timestamp", Instant.now().toString());
            String executedQuery = extractExecutedQuery(result);
            if (executedQuery != null) {
                response.put("executed_query", executedQuery);
            }
            response.put("results", stripExecutionMetadata(result));
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            if (e instanceof QueryException queryException) {
                throw queryException;
            }
            log.error("Error in handleQuery: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "errorType", "INTERNAL_SERVER_ERROR",
                    "message", e.getMessage(),
                    "timestamp", Instant.now().toString()
            ));
        }
    }

    // ============================================================
    // 2️⃣ WRITE ENDPOINT (Supports Single Query or Saga)
    // ============================================================
    @PostMapping("/write")
    public ResponseEntity<?> handleWriteQuery(@Valid @RequestBody Map<String, Object> requestBody) {
        try {
            // Check for Saga-style input
            if (requestBody.containsKey("steps")) {
                log.info("🌀 Saga WRITE request received.");

                List<Map<String, Object>> stepMaps = extractStepMaps(requestBody.get("steps"));

                List<SagaStep> steps = stepMaps.stream()
                        .map(m -> new SagaStep(
                                (String) m.get("question"),
                                (String) m.get("database"),
                                null,
                                null
                        ))
                        .collect(Collectors.toList());

                List<Map<String, Object>> result = orchestratorService.processSaga(steps, true);
                Map<String, Object> response = new LinkedHashMap<>();
                response.put("mode", "saga-write");
                response.put("timestamp", Instant.now().toString());
                response.put("saga_step_count", steps.size());
                response.put("executed_queries", steps.stream()
                        .map(SagaStep::getForwardQuery)
                        .filter(Objects::nonNull)
                        .toList());
                response.put("results", stripExecutionMetadata(result));
                return ResponseEntity.ok(response);
            }

            // ✅ Legacy single write query
            QueryRequest request = new QueryRequest();
            request.setQuestion((String) requestBody.get("question"));
            request.setDatabase((String) requestBody.get("database"));

            String strategy = request.getDatabase() != null
                    ? request.getDatabase().toLowerCase()
                    : rosettixConfiguration.getDefaultStrategy();

            List<Map<String, Object>> result =
                    orchestratorService.processWriteQuery(request.getQuestion(), strategy);
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("mode", "single-write");
            response.put("strategy", strategy);
            response.put("timestamp", Instant.now().toString());
            String executedQuery = extractExecutedQuery(result);
            if (executedQuery != null) {
                response.put("executed_query", executedQuery);
            }
            response.put("results", stripExecutionMetadata(result));
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            if (e instanceof QueryException queryException) {
                throw queryException;
            }
            log.error("Error in handleWriteQuery: {}", e.getMessage(), e);
            return ResponseEntity.internalServerError().body(Map.of(
                    "errorType", "INTERNAL_SERVER_ERROR",
                    "message", e.getMessage(),
                    "timestamp", Instant.now().toString()
            ));
        }
    }

    // ============================================================
    // 3️⃣ AVAILABLE STRATEGIES ENDPOINT
    // ============================================================
    @GetMapping("/strategies")
    public ResponseEntity<Map<String, Object>> getAvailableStrategies() {
        Set<String> strategies = orchestratorService.getAvailableStrategies();
        return ResponseEntity.ok(Map.of(
                "available_strategies", strategies,
                "default_strategy", rosettixConfiguration.getDefaultStrategy(),
                "timestamp", Instant.now().toString()
        ));
    }

    private List<Map<String, Object>> extractStepMaps(Object stepsObject) {
        if (!(stepsObject instanceof List<?> rawSteps)) {
            throw new IllegalArgumentException("'steps' must be a list");
        }

        List<Map<String, Object>> stepMaps = new ArrayList<>();
        for (Object rawStep : rawSteps) {
            if (!(rawStep instanceof Map<?, ?> rawMap)) {
                throw new IllegalArgumentException("Each saga step must be an object");
            }

            Map<String, Object> typedMap = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                if (!(entry.getKey() instanceof String key)) {
                    throw new IllegalArgumentException("Saga step keys must be strings");
                }
                typedMap.put(key, entry.getValue());
            }
            stepMaps.add(typedMap);
        }

        return stepMaps;
    }

    private String extractExecutedQuery(List<Map<String, Object>> result) {
        return result.stream()
                .findFirst()
                .map(row -> row.get("_executed_query"))
                .filter(String.class::isInstance)
                .map(String.class::cast)
                .orElse(null);
    }

    private List<Map<String, Object>> stripExecutionMetadata(List<Map<String, Object>> result) {
        return result.stream()
                .map(row -> {
                    Map<String, Object> copy = new LinkedHashMap<>(row);
                    copy.remove("_executed_query");
                    return copy;
                })
                .filter(copy -> !copy.isEmpty())
                .toList();
    }
}
