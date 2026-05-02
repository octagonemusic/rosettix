package com.rosettix.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryGenerationCacheServiceTest {

    @Test
    void reusesSameExactCacheKeyForNormalizedEquivalentQuestions() {
        CacheTestContext context = cacheService(true, false, Map.of());
        QueryStrategy strategy = strategy("postgres");

        String firstKey = context.service.buildExactCacheKey(" Show   all USERS ", strategy, "users(id);");
        String secondKey = context.service.buildExactCacheKey("show all users", strategy, "users(id);");

        assertEquals(firstKey, secondKey);
    }

    @Test
    void skipsCacheWhenDisabled() {
        CacheTestContext context = cacheService(false, false, Map.of());
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch cachedQuery = context.service.findCachedQuery("show users", strategy, "users(id);");
        assertNull(cachedQuery);

        Map<String, Object> snapshot = context.service.getMetricsSnapshot();
        @SuppressWarnings("unchecked")
        Map<String, Object> strategies = (Map<String, Object>) snapshot.get("strategies");
        @SuppressWarnings("unchecked")
        Map<String, Object> postgres = (Map<String, Object>) strategies.get("postgres");
        assertEquals(1L, postgres.get("cache_bypasses"));
    }

    @Test
    void reusesSemanticallySimilarPromptWhenThresholdMatches() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch match = context.service.findCachedQuery("list every user", strategy, "users(id);");

        assertNotNull(match);
        assertEquals(QueryGenerationCacheMatch.MatchType.SEMANTIC, match.matchType());
        assertEquals("SELECT * FROM users", match.query());
    }

    @Test
    void isolatesSemanticMatchesBySchemaHash() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch match = context.service.findCachedQuery("list every user", strategy, "orders(id);");

        assertNull(match);
    }

    @Test
    void isolatesSemanticMatchesByStrategy() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        QueryStrategy postgresStrategy = strategy("postgres");
        QueryStrategy mongoStrategy = strategy("mongodb");

        context.service.cacheQuery("show all users", postgresStrategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch match = context.service.findCachedQuery("list every user", mongoStrategy, "users(id);");

        assertNull(match);
    }

    @Test
    void recordsExactAndSemanticMetricsSeparately() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");
        QueryGenerationCacheMatch exact = context.service.findCachedQuery("show all users", strategy, "users(id);");
        QueryGenerationCacheMatch semantic = context.service.findCachedQuery("list every user", strategy, "users(id);");

        assertEquals(QueryGenerationCacheMatch.MatchType.EXACT, exact.matchType());
        assertEquals(QueryGenerationCacheMatch.MatchType.SEMANTIC, semantic.matchType());

        @SuppressWarnings("unchecked")
        Map<String, Object> strategies = (Map<String, Object>) context.service.getMetricsSnapshot().get("strategies");
        @SuppressWarnings("unchecked")
        Map<String, Object> postgres = (Map<String, Object>) strategies.get("postgres");

        assertEquals(1L, postgres.get("exact_hits"));
        assertEquals(1L, postgres.get("semantic_hits"));
        assertEquals(0L, postgres.get("cache_misses"));
        assertEquals(1L, postgres.get("exact_writes"));
        assertEquals(1L, postgres.get("semantic_writes"));
        assertEquals(2L, postgres.get("total_lookups"));
        assertTrue((Double) postgres.get("avg_semantic_similarity") > 0.9);
        assertEquals(100.0, (Double) postgres.get("hit_rate_percent"));
    }

    @Test
    void returnsMissWhenSemanticSimilarityFallsBelowThreshold() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.2f, 0.8f)
                )
        );
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch match = context.service.findCachedQuery("list every user", strategy, "users(id);");

        assertNull(match);

        @SuppressWarnings("unchecked")
        Map<String, Object> strategies = (Map<String, Object>) context.service.getMetricsSnapshot().get("strategies");
        @SuppressWarnings("unchecked")
        Map<String, Object> postgres = (Map<String, Object>) strategies.get("postgres");
        assertEquals(1L, postgres.get("cache_misses"));
        assertEquals(0L, postgres.get("semantic_hits"));
    }

    @Test
    void removesStaleSemanticEntriesFromIndexAndRecordsEviction() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of("list every user", vector(0.99f, 0.01f))
        );
        QueryStrategy strategy = strategy("postgres");
        String schemaHash = context.service.schemaHash("users(id);");
        String indexKey = context.service.buildSemanticIndexKey(strategy, schemaHash);

        context.setStore.computeIfAbsent(indexKey, ignored -> new LinkedHashSet<>())
                .add("rosettix:querygen:postgres:semantic:read:" + schemaHash + ":missing");

        QueryGenerationCacheMatch match = context.service.findCachedQuery("list every user", strategy, "users(id);");

        assertNull(match);
        assertTrue(context.setStore.getOrDefault(indexKey, Set.of()).isEmpty());

        @SuppressWarnings("unchecked")
        Map<String, Object> strategies = (Map<String, Object>) context.service.getMetricsSnapshot().get("strategies");
        @SuppressWarnings("unchecked")
        Map<String, Object> postgres = (Map<String, Object>) strategies.get("postgres");
        assertEquals(1L, postgres.get("stale_index_evictions"));
        assertEquals(1L, postgres.get("cache_misses"));
    }

    @Test
    void resetClearsQueryCacheKeysAndMetrics() {
        CacheTestContext context = cacheService(
                true,
                true,
                Map.of("show all users", vector(1.0f, 0.0f))
        );
        QueryStrategy strategy = strategy("postgres");

        context.service.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");
        context.service.findCachedQuery("show all users", strategy, "users(id);");

        Map<String, Object> reset = context.service.reset();

        assertTrue(((Long) reset.get("deleted_keys")) >= 3L);
        assertTrue(context.valueStore.isEmpty());
        assertTrue(context.setStore.isEmpty());

        @SuppressWarnings("unchecked")
        Map<String, Object> strategies = (Map<String, Object>) context.service.getMetricsSnapshot().get("strategies");
        assertTrue(strategies.isEmpty());
    }

    private CacheTestContext cacheService(
            boolean enabled,
            boolean semanticEnabled,
            Map<String, List<Float>> embeddings
    ) {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getQuery().setCachingEnabled(enabled);
        configuration.getQuery().setCacheTtlMinutes(30);
        configuration.getQuery().setSemanticMatchingEnabled(semanticEnabled);
        configuration.getQuery().setSemanticThreshold(0.92);
        configuration.getQuery().setSemanticMaxCandidates(25);

        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        SetOperations<String, String> setOperations = mock(SetOperations.class);
        Map<String, String> valueStore = new ConcurrentHashMap<>();
        Map<String, Set<String>> setStore = new ConcurrentHashMap<>();

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(valueOperations.get(anyString())).thenAnswer(invocation -> valueStore.get(invocation.getArgument(0)));
        doAnswer(invocation -> {
            valueStore.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(valueOperations).set(anyString(), anyString(), any());
        when(setOperations.members(anyString())).thenAnswer(invocation ->
                new LinkedHashSet<>(setStore.getOrDefault(invocation.getArgument(0), Set.of()))
        );
        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            String value = invocation.getArgument(1);
            setStore.computeIfAbsent(key, ignored -> new LinkedHashSet<>()).add(value);
            return 1L;
        }).when(setOperations).add(anyString(), anyString());
        doAnswer(invocation -> {
            String key = invocation.getArgument(0);
            String value = invocation.getArgument(1);
            setStore.computeIfAbsent(key, ignored -> new LinkedHashSet<>()).remove(value);
            return 1L;
        }).when(setOperations).remove(anyString(), anyString());
        when(redisTemplate.expire(anyString(), any())).thenReturn(true);
        when(redisTemplate.keys(anyString())).thenAnswer(invocation -> {
            String pattern = invocation.getArgument(0);
            String prefix = pattern.substring(0, pattern.length() - 1);
            Set<String> keys = new LinkedHashSet<>();
            valueStore.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(keys::add);
            setStore.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(keys::add);
            return keys;
        });
        doAnswer(invocation -> {
            Collection<String> keys = invocation.getArgument(0);
            long removed = 0L;
            for (String key : keys) {
                if (valueStore.remove(key) != null) {
                    removed++;
                }
                if (setStore.remove(key) != null) {
                    removed++;
                }
            }
            return removed;
        }).when(redisTemplate).delete(any(Collection.class));

        EmbeddingService embeddingService = mock(EmbeddingService.class);
        when(embeddingService.embedText(anyString())).thenAnswer(invocation -> {
            String text = invocation.getArgument(0);
            if (!embeddings.containsKey(text)) {
                throw new IllegalStateException("No embedding stub configured for: " + text);
            }
            return embeddings.get(text);
        });

        return new CacheTestContext(
                new QueryGenerationCacheService(
                        configuration,
                        redisTemplate,
                        embeddingService,
                        new ObjectMapper(),
                        Clock.fixed(Instant.parse("2026-05-02T10:15:30Z"), ZoneOffset.UTC)
                ),
                valueStore,
                setStore
        );
    }

    private QueryStrategy strategy(String strategyName) {
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn(strategyName);
        return strategy;
    }

    private List<Float> vector(float... values) {
        List<Float> vector = new java.util.ArrayList<>();
        for (float value : values) {
            vector.add(value);
        }
        return vector;
    }

    private record CacheTestContext(
            QueryGenerationCacheService service,
            Map<String, String> valueStore,
            Map<String, Set<String>> setStore
    ) {
    }
}
