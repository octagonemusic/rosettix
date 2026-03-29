package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

@Service
@Slf4j
public class SchemaCacheService {

    private final RosettixConfiguration configuration;
    private final Clock clock;
    private final Map<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();
    private final Map<String, SchemaCacheStats> metrics = new ConcurrentHashMap<>();

    @Autowired
    public SchemaCacheService(RosettixConfiguration configuration) {
        this(configuration, Clock.systemUTC());
    }

    SchemaCacheService(RosettixConfiguration configuration, Clock clock) {
        this.configuration = configuration;
        this.clock = clock;
    }

    public String getSchema(String key, Supplier<String> loader) {
        RosettixConfiguration.SchemaCacheConfig schemaCacheConfig = configuration.getSchemaCache();
        long startNanos = System.nanoTime();

        if (!schemaCacheConfig.isEnabled()) {
            log.info("Schema cache disabled, fetching schema: {}", key);
            String schema = loader.get();
            getStats(key).recordBypass(System.nanoTime() - startNanos);
            return schema;
        }

        CachedSchema cachedSchema = schemaCache.get(key);
        if (cachedSchema != null && !isExpired(cachedSchema, schemaCacheConfig.getTtlMinutes())) {
            long elapsedNanos = System.nanoTime() - startNanos;
            SchemaCacheStats stats = getStats(key);
            stats.recordHit(elapsedNanos);
            log.info("Cache hit for schema: {}", key);
            return cachedSchema.schema();
        }

        log.info("Cache miss, fetching schema: {}", key);
        String schema = loader.get();
        schemaCache.put(key, new CachedSchema(schema, Instant.now(clock)));
        SchemaCacheStats stats = getStats(key);
        stats.recordMiss(System.nanoTime() - startNanos);
        Double latencyReduction = stats.getEstimatedLatencyReductionPercent();
        if (latencyReduction != null) {
            log.info("Schema caching reduced latency by {}% for {}", formatReduction(latencyReduction), key);
        }
        return schema;
    }

    public Map<String, Object> getMetricsSnapshot() {
        RosettixConfiguration.SchemaCacheConfig schemaCacheConfig = configuration.getSchemaCache();
        Map<String, Object> databases = new LinkedHashMap<>();

        for (Map.Entry<String, SchemaCacheStats> entry : metrics.entrySet()) {
            databases.put(entry.getKey(), entry.getValue().toSnapshot());
        }

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("enabled", schemaCacheConfig.isEnabled());
        response.put("ttl_minutes", schemaCacheConfig.getTtlMinutes());
        response.put("databases", databases);
        response.put("overall", aggregateSnapshot());
        response.put("timestamp", Instant.now(clock).toString());
        return response;
    }

    private boolean isExpired(CachedSchema cachedSchema, long ttlMinutes) {
        Duration age = Duration.between(cachedSchema.cachedAt(), Instant.now(clock));
        return age.compareTo(Duration.ofMinutes(ttlMinutes)) > 0;
    }

    private SchemaCacheStats getStats(String key) {
        return metrics.computeIfAbsent(key, ignored -> new SchemaCacheStats());
    }

    private Map<String, Object> aggregateSnapshot() {
        SchemaCacheStats aggregate = new SchemaCacheStats();
        metrics.values().forEach(aggregate::mergeFrom);
        return aggregate.toSnapshot();
    }

    private String formatReduction(double value) {
        return String.format("%.2f", value);
    }

    record CachedSchema(String schema, Instant cachedAt) {
    }

    static final class SchemaCacheStats {
        private final LongAdder hits = new LongAdder();
        private final LongAdder misses = new LongAdder();
        private final LongAdder bypasses = new LongAdder();
        private final LongAdder totalHitNanos = new LongAdder();
        private final LongAdder totalMissNanos = new LongAdder();
        private final LongAdder totalBypassNanos = new LongAdder();

        void recordHit(long elapsedNanos) {
            hits.increment();
            totalHitNanos.add(elapsedNanos);
        }

        void recordMiss(long elapsedNanos) {
            misses.increment();
            totalMissNanos.add(elapsedNanos);
        }

        void recordBypass(long elapsedNanos) {
            bypasses.increment();
            totalBypassNanos.add(elapsedNanos);
        }

        void mergeFrom(SchemaCacheStats other) {
            hits.add(other.hits.sum());
            misses.add(other.misses.sum());
            bypasses.add(other.bypasses.sum());
            totalHitNanos.add(other.totalHitNanos.sum());
            totalMissNanos.add(other.totalMissNanos.sum());
            totalBypassNanos.add(other.totalBypassNanos.sum());
        }

        Map<String, Object> toSnapshot() {
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("cache_hits", hits.sum());
            snapshot.put("cache_misses", misses.sum());
            snapshot.put("cache_bypasses", bypasses.sum());
            snapshot.put("avg_hit_ms", nanosToMillis(totalHitNanos.sum(), hits.sum()));
            snapshot.put("avg_miss_ms", nanosToMillis(totalMissNanos.sum(), misses.sum()));
            snapshot.put("avg_bypass_ms", nanosToMillis(totalBypassNanos.sum(), bypasses.sum()));
            snapshot.put("estimated_latency_reduction_percent", getEstimatedLatencyReductionPercent());
            return snapshot;
        }

        Double getEstimatedLatencyReductionPercent() {
            double avgMissMs = nanosToMillis(totalMissNanos.sum(), misses.sum());
            double avgHitMs = nanosToMillis(totalHitNanos.sum(), hits.sum());

            if (avgMissMs <= 0 || hits.sum() == 0 || misses.sum() == 0) {
                return null;
            }

            return ((avgMissMs - avgHitMs) / avgMissMs) * 100.0;
        }

        private double nanosToMillis(long totalNanos, long samples) {
            if (samples == 0) {
                return 0.0;
            }
            return (totalNanos / 1_000_000.0) / samples;
        }
    }
}
