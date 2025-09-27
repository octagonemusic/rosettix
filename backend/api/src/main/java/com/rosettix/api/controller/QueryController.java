package com.rosettix.api.controller;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.dto.QueryRequest;
import com.rosettix.api.service.OrchestratorService;
import jakarta.validation.Valid;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/query")
@RequiredArgsConstructor
public class QueryController {

    private final OrchestratorService orchestratorService;
    private final RosettixConfiguration rosettixConfiguration;

    @PostMapping
    public List<Map<String, Object>> handleQuery(
        @Valid @RequestBody QueryRequest request
    ) {
        // Use configured default strategy if none specified
        String strategy = request.getDatabase() != null
            ? request.getDatabase()
            : rosettixConfiguration.getDefaultStrategy();

        return orchestratorService.processQuery(
            request.getQuestion(),
            strategy
        );
    }

    @GetMapping("/strategies")
    public Set<String> getAvailableStrategies() {
        return orchestratorService.getAvailableStrategies();
    }
}
