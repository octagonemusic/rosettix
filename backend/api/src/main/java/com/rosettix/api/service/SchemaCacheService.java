package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

@Service
@Slf4j
public class SchemaCacheService {

    private final RosettixConfiguration configuration;
    private final SchemaCacheStore schemaCacheStore;
    private final Clock clock;
    private final Map<String, CompletableFuture<String>> inFlightLoads = new ConcurrentHashMap<>();
    private final Map<String, SchemaCacheStats> metrics = new ConcurrentHashMap<>();

    @Autowired
    public SchemaCacheService(RosettixConfiguration configuration, SchemaCacheStore schemaCacheStore) {
        this(configuration, schemaCacheStore, Clock.systemUTC());
    }

    public SchemaCacheService(RosettixConfiguration configuration, SchemaCacheStore schemaCacheStore, Clock clock) {
        this.configuration = configuration;
        this.schemaCacheStore = schemaCacheStore;
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

        String cachedSchema = readFromCache(key);
        if (cachedSchema != null) {
            long elapsedNanos = System.nanoTime() - startNanos;
            SchemaCacheStats stats = getStats(key);
            stats.recordHit(elapsedNanos);
            log.info("Cache hit for schema: {}", key);
            return cachedSchema;
        }

        CompletableFuture<String> newLoad = new CompletableFuture<>();
        CompletableFuture<String> inFlightLoad = inFlightLoads.putIfAbsent(key, newLoad);

        if (inFlightLoad == null) {
            try {
                log.info("Cache miss, fetching schema: {}", key);
                String schema = loader.get();
                writeToCache(key, schema, Duration.ofMinutes(schemaCacheConfig.getTtlMinutes()));
                SchemaCacheStats stats = getStats(key);
                stats.recordMiss(System.nanoTime() - startNanos);
                Double latencyReduction = stats.getEstimatedLatencyReductionPercent();
                if (latencyReduction != null) {
                    log.info("Schema caching reduced latency by {}% for {}", formatReduction(latencyReduction), key);
                }
                newLoad.complete(schema);
                return schema;
            } catch (Exception e) {
                newLoad.completeExceptionally(e);
                throw e;
            } finally {
                inFlightLoads.remove(key, newLoad);
            }
        }

        try {
            log.info("Schema load already in flight, waiting for existing fetch: {}", key);
            String schema = inFlightLoad.join();
            getStats(key).recordInFlightWait(System.nanoTime() - startNanos);
            return schema;
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new RuntimeException("Schema load failed for " + key, cause);
        }
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
        response.put("store", schemaCacheStore.getStoreType());
        response.put("databases", databases);
        response.put("overall", aggregateSnapshot());
        response.put("timestamp", Instant.now(clock).toString());
        return response;
    }

    private SchemaCacheStats getStats(String key) {
        return metrics.computeIfAbsent(key, ignored -> new SchemaCacheStats());
    }

    private String readFromCache(String key) {
        try {
            return schemaCacheStore.get(key);
        } catch (Exception e) {
            log.warn("Unable to read schema cache entry for {} from {}: {}", key, schemaCacheStore.getStoreType(), e.getMessage());
            return null;
        }
    }

    private void writeToCache(String key, String schema, Duration ttl) {
        try {
            schemaCacheStore.put(key, schema, ttl);
        } catch (Exception e) {
            log.warn("Unable to write schema cache entry for {} to {}: {}", key, schemaCacheStore.getStoreType(), e.getMessage());
        }
    }

    private Map<String, Object> aggregateSnapshot() {
        SchemaCacheStats aggregate = new SchemaCacheStats();
        metrics.values().forEach(aggregate::mergeFrom);
        return aggregate.toSnapshot();
    }

    private String formatReduction(double value) {
        return String.format("%.2f", value);
    }

    static final class SchemaCacheStats {
        private final LongAdder hits = new LongAdder();
        private final LongAdder misses = new LongAdder();
        private final LongAdder bypasses = new LongAdder();
        private final LongAdder inFlightWaits = new LongAdder();
        private final LongAdder totalHitNanos = new LongAdder();
        private final LongAdder totalMissNanos = new LongAdder();
        private final LongAdder totalBypassNanos = new LongAdder();
        private final LongAdder totalInFlightWaitNanos = new LongAdder();

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

        void recordInFlightWait(long elapsedNanos) {
            inFlightWaits.increment();
            totalInFlightWaitNanos.add(elapsedNanos);
        }

        void mergeFrom(SchemaCacheStats other) {
            hits.add(other.hits.sum());
            misses.add(other.misses.sum());
            bypasses.add(other.bypasses.sum());
            inFlightWaits.add(other.inFlightWaits.sum());
            totalHitNanos.add(other.totalHitNanos.sum());
            totalMissNanos.add(other.totalMissNanos.sum());
            totalBypassNanos.add(other.totalBypassNanos.sum());
            totalInFlightWaitNanos.add(other.totalInFlightWaitNanos.sum());
        }

        Map<String, Object> toSnapshot() {
            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("cache_hits", hits.sum());
            snapshot.put("cache_misses", misses.sum());
            snapshot.put("cache_bypasses", bypasses.sum());
            snapshot.put("in_flight_waits", inFlightWaits.sum());
            snapshot.put("avg_hit_ms", nanosToMillis(totalHitNanos.sum(), hits.sum()));
            snapshot.put("avg_miss_ms", nanosToMillis(totalMissNanos.sum(), misses.sum()));
            snapshot.put("avg_bypass_ms", nanosToMillis(totalBypassNanos.sum(), bypasses.sum()));
            snapshot.put("avg_in_flight_wait_ms", nanosToMillis(totalInFlightWaitNanos.sum(), inFlightWaits.sum()));
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
