package com.rosettix.api.controller;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.dto.QueryRequest;
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

                List<Map<String, Object>> stepMaps = (List<Map<String, Object>>) requestBody.get("steps");

                List<SagaStep> steps = stepMaps.stream()
                        .map(m -> new SagaStep(
                                (String) m.get("question"),
                                (String) m.get("database"),
                                null,
                                null
                        ))
                        .collect(Collectors.toList());

                List<Map<String, Object>> result = orchestratorService.processSaga(steps, false);

                return ResponseEntity.ok(Map.of(
                        "mode", "saga-read",
                        "timestamp", Instant.now().toString(),
                        "saga_step_count", steps.size(),
                        "results", result
                ));
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

            return ResponseEntity.ok(Map.of(
                    "mode", "single-read",
                    "strategy", strategy,
                    "timestamp", Instant.now().toString(),
                    "results", result
            ));

        } catch (Exception e) {
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

                List<Map<String, Object>> stepMaps = (List<Map<String, Object>>) requestBody.get("steps");

                List<SagaStep> steps = stepMaps.stream()
                        .map(m -> new SagaStep(
                                (String) m.get("question"),
                                (String) m.get("database"),
                                null,
                                null
                        ))
                        .collect(Collectors.toList());

                List<Map<String, Object>> result = orchestratorService.processSaga(steps, true);

                return ResponseEntity.ok(Map.of(
                        "mode", "saga-write",
                        "timestamp", Instant.now().toString(),
                        "saga_step_count", steps.size(),
                        "results", result
                ));
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

            return ResponseEntity.ok(Map.of(
                    "mode", "single-write",
                    "strategy", strategy,
                    "timestamp", Instant.now().toString(),
                    "results", result
            ));

        } catch (Exception e) {
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
}