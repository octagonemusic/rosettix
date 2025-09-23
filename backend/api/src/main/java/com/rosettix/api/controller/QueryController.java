package com.rosettix.api.controller;

import com.rosettix.api.dto.QueryRequest;
import com.rosettix.api.service.OrchestratorService;
import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/query")
public class QueryController {

    @Autowired
    private OrchestratorService orchestratorService;

    @PostMapping
    public List<Map<String, Object>> handleQuery(
        @RequestBody QueryRequest request
    ) {
        return orchestratorService.processQuery(request.getQuestion());
    }
}
