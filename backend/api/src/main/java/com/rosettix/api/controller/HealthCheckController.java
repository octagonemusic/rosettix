package com.rosettix.api.controller;

import java.util.HashMap;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@Slf4j
public class HealthCheckController {

    private final JdbcTemplate jdbcTemplate;
    private final MongoTemplate mongoTemplate;

    @GetMapping("/health")
    public String healthCheck() {
        return "OK";
    }

    @GetMapping("/health/database")
    public Map<String, Object> databaseHealthCheck() {
        Map<String, Object> health = new HashMap<>();

        // Test PostgreSQL
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            health.put(
                "postgres",
                Map.of("status", "UP", "message", "Connection successful")
            );
        } catch (Exception e) {
            health.put(
                "postgres",
                Map.of("status", "DOWN", "message", e.getMessage())
            );
        }

        // Test MongoDB
        try {
            mongoTemplate.getDb().runCommand(new Document("ping", 1));
            health.put(
                "mongodb",
                Map.of(
                    "status",
                    "UP",
                    "message",
                    "Connection successful",
                    "database",
                    mongoTemplate.getDb().getName()
                )
            );
        } catch (Exception e) {
            health.put(
                "mongodb",
                Map.of("status", "DOWN", "message", e.getMessage())
            );
        }

        return health;
    }
}
