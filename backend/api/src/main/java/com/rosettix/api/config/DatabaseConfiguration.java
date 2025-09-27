package com.rosettix.api.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.HashMap;

/**
 * Configuration class for managing database strategy settings
 */
@Configuration
@ConfigurationProperties(prefix = "rosettix.databases")
@Data
public class DatabaseConfiguration {

    private Map<String, DatabaseConfig> strategies = new HashMap<>();

    @Data
    public static class DatabaseConfig {
        private boolean enabled = true;
        private String url;
        private String username;
        private String password;
        private String database;
        private Map<String, String> properties = new HashMap<>();

        /**
         * Check if this database configuration is valid and enabled
         */
        public boolean isValid() {
            return enabled && url != null && !url.trim().isEmpty();
        }
    }

    /**
     * Get configuration for a specific database strategy
     */
    public DatabaseConfig getStrategy(String strategyName) {
        return strategies.get(strategyName);
    }

    /**
     * Check if a strategy is enabled and properly configured
     */
    public boolean isStrategyEnabled(String strategyName) {
        DatabaseConfig config = strategies.get(strategyName);
        return config != null && config.isValid();
    }

    /**
     * Get all enabled strategy names
     */
    public java.util.Set<String> getEnabledStrategies() {
        return strategies.entrySet().stream()
                .filter(entry -> entry.getValue().isValid())
                .map(Map.Entry::getKey)
                .collect(java.util.stream.Collectors.toSet());
    }
}
