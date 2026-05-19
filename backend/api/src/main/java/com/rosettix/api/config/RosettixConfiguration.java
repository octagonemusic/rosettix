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

    /**
     * Schema cache configuration settings
     */
    private SchemaCacheConfig schemaCache = new SchemaCacheConfig();

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
         * Whether to enable generated query caching for read requests
         */
        private boolean cachingEnabled = false;

        /**
         * Generated query cache TTL in minutes
         */
        private long cacheTtlMinutes = 30;

        /**
         * Whether semantic prompt matching is enabled for read query caching
         */
        private boolean semanticMatchingEnabled = false;

        /**
         * Minimum cosine similarity required to reuse a semantically similar cached query
         */
        private double semanticThreshold = 0.92;

        /**
         * Maximum cached semantic candidates to evaluate per strategy/schema bucket
         */
        private int semanticMaxCandidates = 25;

        /**
         * Whether to initialize the pgvector semantic cache schema on startup
         */
        private boolean semanticPgvectorAutoInit = true;
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

        /**
         * Model name to use for embedding generation
         */
        private String embeddingModelName = "gemini-embedding-001";

        /**
         * Output dimensionality to request from the embedding model
         */
        private int embeddingDimensions = 768;
    }

    @Data
    public static class SchemaCacheConfig {
        /**
         * Whether schema caching is enabled
         */
        private boolean enabled = true;

        /**
         * Schema cache TTL in minutes
         */
        private long ttlMinutes = 5;
    }
}
