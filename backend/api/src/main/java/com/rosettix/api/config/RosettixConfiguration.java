package com.rosettix.api.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration class for Rosettix application settings
 */
@Configuration
@ConfigurationProperties(prefix = "rosettix")
@Data
public class RosettixConfiguration {

    /**
     * Default database strategy to use when none is specified
     */
    private String defaultStrategy = "postgres";

    /**
     * Query configuration settings
     */
    private QueryConfig query = new QueryConfig();

    /**
     * LLM configuration settings
     */
    private LlmConfig llm = new LlmConfig();

    @Data
    public static class QueryConfig {
        /**
         * Maximum allowed query result size
         */
        private int maxResultSize = 1000;

        /**
         * Query timeout in seconds
         */
        private int timeoutSeconds = 30;

        /**
         * Whether to enable query caching
         */
        private boolean cachingEnabled = false;
    }

    @Data
    public static class LlmConfig {
        /**
         * Model name to use for query generation
         */
        private String modelName = "gemini-2.5-flash";

        /**
         * Maximum retry attempts for LLM calls
         */
        private int maxRetries = 3;

        /**
         * Timeout for LLM calls in seconds
         */
        private int timeoutSeconds = 30;
    }
}