package com.rosettix.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.concurrent.atomic.LongAdder;

@Service
@Slf4j
public class QueryGenerationCacheService {

    static final String KEY_PREFIX = "rosettix:querygen:";

    private final RosettixConfiguration configuration;
    private final StringRedisTemplate redisTemplate;
    private final EmbeddingService embeddingService;
    private final ObjectMapper objectMapper;
    private final Clock clock;
    private final Map<String, QueryCacheStats> metrics = new ConcurrentHashMap<>();

    @Autowired
    public QueryGenerationCacheService(
            RosettixConfiguration configuration,
            StringRedisTemplate redisTemplate,
            EmbeddingService embeddingService,
            ObjectMapper objectMapper
    ) {
        this(configuration, redisTemplate, embeddingService, objectMapper, Clock.systemUTC());
    }

    QueryGenerationCacheService(
            RosettixConfiguration configuration,
            StringRedisTemplate redisTemplate,
            EmbeddingService embeddingService,
            ObjectMapper objectMapper,
            Clock clock
    ) {
        this.configuration = configuration;
        this.redisTemplate = redisTemplate;
        this.embeddingService = embeddingService;
        this.objectMapper = objectMapper;
        this.clock = clock;
    }

    public QueryGenerationCacheMatch findCachedQuery(String question, QueryStrategy strategy, String schemaRepresentation) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        String strategyName = strategy.getStrategyName();
        long startNanos = System.nanoTime();
        if (!queryConfig.isCachingEnabled()) {
            getStats(strategyName).recordBypass(System.nanoTime() - startNanos);
            return null;
        }

        String key = buildExactCacheKey(question, strategy, schemaRepresentation);
        try {
            String cachedQuery = redisTemplate.opsForValue().get(key);
            if (cachedQuery != null) {
                getStats(strategyName).recordExactHit(System.nanoTime() - startNanos);
                log.info("Query generation exact cache hit for strategy {}", strategyName);
                return new QueryGenerationCacheMatch(
                        cachedQuery,
                        QueryGenerationCacheMatch.MatchType.EXACT,
                        1.0
                );
            }
        } catch (Exception e) {
            getStats(strategyName).recordLookupFailure();
            log.warn("Unable to read generated query cache entry for {}: {}", strategyName, e.getMessage());
        }

        if (!queryConfig.isSemanticMatchingEnabled()) {
            getStats(strategyName).recordMiss(System.nanoTime() - startNanos);
            return null;
        }

        QueryGenerationCacheMatch semanticMatch = findSemanticMatch(question, strategy, schemaRepresentation);
        QueryCacheStats stats = getStats(strategyName);
        long elapsedNanos = System.nanoTime() - startNanos;

        if (semanticMatch != null) {
            stats.recordSemanticHit(elapsedNanos, semanticMatch.similarityScore());
            return semanticMatch;
        }

        stats.recordMiss(elapsedNanos);
        return null;
    }

    public void cacheQuery(String question, QueryStrategy strategy, String schemaRepresentation, String cleanedQuery) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        if (!queryConfig.isCachingEnabled() || cleanedQuery == null || cleanedQuery.isBlank()) {
            return;
        }

        String schemaHash = schemaHash(schemaRepresentation);
        String key = buildExactCacheKey(question, strategy, schemaRepresentation);
        Duration ttl = Duration.ofMinutes(queryConfig.getCacheTtlMinutes());
        QueryCacheStats stats = getStats(strategy.getStrategyName());
        long writeStartNanos = System.nanoTime();

        try {
            redisTemplate.opsForValue().set(key, cleanedQuery, ttl);
            stats.recordExactWrite(System.nanoTime() - writeStartNanos);
            log.info("Stored generated query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            stats.recordWriteFailure();
            log.warn("Unable to write generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }

        if (queryConfig.isSemanticMatchingEnabled()) {
            storeSemanticEntry(question, strategy, schemaHash, cleanedQuery, ttl);
        }
    }

    public Map<String, Object> getMetricsSnapshot() {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        Map<String, Object> strategies = new LinkedHashMap<>();

        for (Map.Entry<String, QueryCacheStats> entry : metrics.entrySet()) {
            strategies.put(entry.getKey(), entry.getValue().toSnapshot());
        }

        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("enabled", queryConfig.isCachingEnabled());
        snapshot.put("semantic_matching_enabled", queryConfig.isSemanticMatchingEnabled());
        snapshot.put("cache_ttl_minutes", queryConfig.getCacheTtlMinutes());
        snapshot.put("semantic_threshold", queryConfig.getSemanticThreshold());
        snapshot.put("semantic_max_candidates", queryConfig.getSemanticMaxCandidates());
        snapshot.put("store", "redis");
        snapshot.put("strategies", strategies);
        snapshot.put("overall", aggregateSnapshot());
        snapshot.put("timestamp", Instant.now(clock).toString());
        return snapshot;
    }

    public Map<String, Object> reset() {
        long deletedKeys = 0L;
        try {
            Set<String> keys = redisTemplate.keys(KEY_PREFIX + "*");
            if (keys != null && !keys.isEmpty()) {
                Long removed = redisTemplate.delete(keys);
                deletedKeys = removed == null ? 0L : removed;
            }
        } catch (Exception e) {
            log.warn("Unable to clear generated query cache keys: {}", e.getMessage());
        }
        metrics.clear();

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("deleted_keys", deletedKeys);
        response.put("timestamp", Instant.now(clock).toString());
        return response;
    }

    String buildExactCacheKey(String question, QueryStrategy strategy, String schemaRepresentation) {
        String normalizedQuestion = normalizeQuestion(question);
        String questionHash = sha256(normalizedQuestion);
        String schemaHash = schemaHash(schemaRepresentation);
        return KEY_PREFIX + strategy.getStrategyName() + ":read:" + schemaHash + ":" + questionHash;
    }

    String buildSemanticEntryKey(String question, QueryStrategy strategy, String schemaHash) {
        String questionHash = sha256(normalizeQuestion(question));
        return KEY_PREFIX + strategy.getStrategyName() + ":semantic:read:" + schemaHash + ":" + questionHash;
    }

    String buildSemanticIndexKey(QueryStrategy strategy, String schemaHash) {
        return KEY_PREFIX + strategy.getStrategyName() + ":semantic:index:read:" + schemaHash;
    }

    String schemaHash(String schemaRepresentation) {
        return sha256(schemaRepresentation == null ? "" : schemaRepresentation);
    }

    String normalizeQuestion(String question) {
        if (question == null) {
            return "";
        }

        return question
                .trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
    }

    private QueryGenerationCacheMatch findSemanticMatch(String question, QueryStrategy strategy, String schemaRepresentation) {
        QueryCacheStats stats = getStats(strategy.getStrategyName());
        try {
            RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
            String schemaHash = schemaHash(schemaRepresentation);
            String normalizedQuestion = normalizeQuestion(question);
            List<Float> inputEmbedding = embeddingService.embedText(normalizedQuestion);
            Set<String> entryKeys = redisTemplate.opsForSet().members(buildSemanticIndexKey(strategy, schemaHash));
            if (entryKeys == null || entryKeys.isEmpty()) {
                return null;
            }

            QueryGenerationCacheMatch bestMatch = null;
            double bestScore = -1.0;
            int processed = 0;
            for (String entryKey : new LinkedHashSet<>(entryKeys)) {
                if (processed >= queryConfig.getSemanticMaxCandidates()) {
                    break;
                }
                processed++;

                String payload = redisTemplate.opsForValue().get(entryKey);
                if (payload == null) {
                    safeRemoveIndexEntry(buildSemanticIndexKey(strategy, schemaHash), entryKey);
                    stats.recordStaleIndexEviction();
                    continue;
                }

                SemanticQueryCacheEntry entry = objectMapper.readValue(payload, SemanticQueryCacheEntry.class);
                double similarity = cosineSimilarity(inputEmbedding, entry.embedding());
                if (similarity >= queryConfig.getSemanticThreshold() && similarity > bestScore) {
                    bestScore = similarity;
                    bestMatch = new QueryGenerationCacheMatch(
                            entry.generatedQuery(),
                            QueryGenerationCacheMatch.MatchType.SEMANTIC,
                            similarity
                    );
                }
            }

            if (bestMatch != null) {
                log.info(
                        "Query generation semantic cache hit for strategy {} with similarity {}",
                        strategy.getStrategyName(),
                        String.format("%.4f", bestMatch.similarityScore())
                );
            }
            return bestMatch;
        } catch (Exception e) {
            stats.recordLookupFailure();
            log.warn("Semantic query cache lookup failed for {}: {}", strategy.getStrategyName(), e.getMessage());
            return null;
        }
    }

    private void storeSemanticEntry(
            String question,
            QueryStrategy strategy,
            String schemaHash,
            String cleanedQuery,
            Duration ttl
    ) {
        QueryCacheStats stats = getStats(strategy.getStrategyName());
        long writeStartNanos = System.nanoTime();
        try {
            String normalizedQuestion = normalizeQuestion(question);
            SemanticQueryCacheEntry entry = new SemanticQueryCacheEntry(
                    strategy.getStrategyName(),
                    schemaHash,
                    normalizedQuestion,
                    cleanedQuery,
                    embeddingService.embedText(normalizedQuestion),
                    Instant.now(clock).toString()
            );
            String entryKey = buildSemanticEntryKey(question, strategy, schemaHash);
            redisTemplate.opsForValue().set(entryKey, objectMapper.writeValueAsString(entry), ttl);

            String indexKey = buildSemanticIndexKey(strategy, schemaHash);
            SetOperations<String, String> setOperations = redisTemplate.opsForSet();
            setOperations.add(indexKey, entryKey);
            redisTemplate.expire(indexKey, ttl);
            stats.recordSemanticWrite(System.nanoTime() - writeStartNanos);
            log.info("Stored semantic query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            stats.recordWriteFailure();
            log.warn("Unable to write semantic query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }
    }

    private void safeRemoveIndexEntry(String indexKey, String entryKey) {
        try {
            redisTemplate.opsForSet().remove(indexKey, entryKey);
        } catch (Exception e) {
            log.debug("Unable to remove stale semantic cache index entry {}: {}", entryKey, e.getMessage());
        }
    }

    private double cosineSimilarity(List<Float> first, List<Float> second) {
        if (first == null || second == null || first.isEmpty() || second.isEmpty()) {
            return -1.0;
        }

        int size = Math.min(first.size(), second.size());
        double dotProduct = 0.0;
        double firstMagnitude = 0.0;
        double secondMagnitude = 0.0;

        for (int i = 0; i < size; i++) {
            double left = first.get(i);
            double right = second.get(i);
            dotProduct += left * right;
            firstMagnitude += left * left;
            secondMagnitude += right * right;
        }

        if (firstMagnitude == 0.0 || secondMagnitude == 0.0) {
            return -1.0;
        }

        return dotProduct / (Math.sqrt(firstMagnitude) * Math.sqrt(secondMagnitude));
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }

    private QueryCacheStats getStats(String strategyName) {
        return metrics.computeIfAbsent(strategyName, ignored -> new QueryCacheStats());
    }

    private Map<String, Object> aggregateSnapshot() {
        QueryCacheStats aggregate = new QueryCacheStats();
        metrics.values().forEach(aggregate::mergeFrom);
        return aggregate.toSnapshot();
    }

    static final class QueryCacheStats {
        private final LongAdder exactHits = new LongAdder();
        private final LongAdder semanticHits = new LongAdder();
        private final LongAdder misses = new LongAdder();
        private final LongAdder bypasses = new LongAdder();
        private final LongAdder lookupFailures = new LongAdder();
        private final LongAdder staleIndexEvictions = new LongAdder();
        private final LongAdder exactWrites = new LongAdder();
        private final LongAdder semanticWrites = new LongAdder();
        private final LongAdder writeFailures = new LongAdder();
        private final LongAdder totalExactHitNanos = new LongAdder();
        private final LongAdder totalSemanticHitNanos = new LongAdder();
        private final LongAdder totalMissNanos = new LongAdder();
        private final LongAdder totalBypassNanos = new LongAdder();
        private final LongAdder totalExactWriteNanos = new LongAdder();
        private final LongAdder totalSemanticWriteNanos = new LongAdder();
        private final DoubleAdder totalSemanticSimilarity = new DoubleAdder();

        void recordExactHit(long elapsedNanos) {
            exactHits.increment();
            totalExactHitNanos.add(elapsedNanos);
        }

        void recordSemanticHit(long elapsedNanos, double similarity) {
            semanticHits.increment();
            totalSemanticHitNanos.add(elapsedNanos);
            totalSemanticSimilarity.add(similarity);
        }

        void recordMiss(long elapsedNanos) {
            misses.increment();
            totalMissNanos.add(elapsedNanos);
        }

        void recordBypass(long elapsedNanos) {
            bypasses.increment();
            totalBypassNanos.add(elapsedNanos);
        }

        void recordLookupFailure() {
            lookupFailures.increment();
        }

        void recordStaleIndexEviction() {
            staleIndexEvictions.increment();
        }

        void recordExactWrite(long elapsedNanos) {
            exactWrites.increment();
            totalExactWriteNanos.add(elapsedNanos);
        }

        void recordSemanticWrite(long elapsedNanos) {
            semanticWrites.increment();
            totalSemanticWriteNanos.add(elapsedNanos);
        }

        void recordWriteFailure() {
            writeFailures.increment();
        }

        void mergeFrom(QueryCacheStats other) {
            exactHits.add(other.exactHits.sum());
            semanticHits.add(other.semanticHits.sum());
            misses.add(other.misses.sum());
            bypasses.add(other.bypasses.sum());
            lookupFailures.add(other.lookupFailures.sum());
            staleIndexEvictions.add(other.staleIndexEvictions.sum());
            exactWrites.add(other.exactWrites.sum());
            semanticWrites.add(other.semanticWrites.sum());
            writeFailures.add(other.writeFailures.sum());
            totalExactHitNanos.add(other.totalExactHitNanos.sum());
            totalSemanticHitNanos.add(other.totalSemanticHitNanos.sum());
            totalMissNanos.add(other.totalMissNanos.sum());
            totalBypassNanos.add(other.totalBypassNanos.sum());
            totalExactWriteNanos.add(other.totalExactWriteNanos.sum());
            totalSemanticWriteNanos.add(other.totalSemanticWriteNanos.sum());
            totalSemanticSimilarity.add(other.totalSemanticSimilarity.sum());
        }

        Map<String, Object> toSnapshot() {
            long totalLookups = exactHits.sum() + semanticHits.sum() + misses.sum();

            Map<String, Object> snapshot = new HashMap<>();
            snapshot.put("exact_hits", exactHits.sum());
            snapshot.put("semantic_hits", semanticHits.sum());
            snapshot.put("cache_misses", misses.sum());
            snapshot.put("cache_bypasses", bypasses.sum());
            snapshot.put("lookup_failures", lookupFailures.sum());
            snapshot.put("stale_index_evictions", staleIndexEvictions.sum());
            snapshot.put("exact_writes", exactWrites.sum());
            snapshot.put("semantic_writes", semanticWrites.sum());
            snapshot.put("write_failures", writeFailures.sum());
            snapshot.put("total_lookups", totalLookups);
            snapshot.put("hit_rate_percent", percentage(exactHits.sum() + semanticHits.sum(), totalLookups));
            snapshot.put("semantic_share_of_hits_percent", percentage(semanticHits.sum(), exactHits.sum() + semanticHits.sum()));
            snapshot.put("avg_exact_hit_ms", nanosToMillis(totalExactHitNanos.sum(), exactHits.sum()));
            snapshot.put("avg_semantic_hit_ms", nanosToMillis(totalSemanticHitNanos.sum(), semanticHits.sum()));
            snapshot.put("avg_miss_ms", nanosToMillis(totalMissNanos.sum(), misses.sum()));
            snapshot.put("avg_bypass_ms", nanosToMillis(totalBypassNanos.sum(), bypasses.sum()));
            snapshot.put("avg_exact_write_ms", nanosToMillis(totalExactWriteNanos.sum(), exactWrites.sum()));
            snapshot.put("avg_semantic_write_ms", nanosToMillis(totalSemanticWriteNanos.sum(), semanticWrites.sum()));
            snapshot.put("avg_semantic_similarity", average(totalSemanticSimilarity.sum(), semanticHits.sum()));
            return snapshot;
        }

        private double nanosToMillis(long totalNanos, long samples) {
            if (samples == 0) {
                return 0.0;
            }
            return (totalNanos / 1_000_000.0) / samples;
        }

        private Double average(double total, long samples) {
            if (samples == 0) {
                return null;
            }
            return total / samples;
        }

        private Double percentage(long numerator, long denominator) {
            if (denominator == 0) {
                return null;
            }
            return (numerator * 100.0) / denominator;
        }
    }
}
