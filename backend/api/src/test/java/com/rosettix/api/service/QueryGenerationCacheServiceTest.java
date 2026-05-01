package com.rosettix.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryGenerationCacheServiceTest {

    @Test
    void reusesSameExactCacheKeyForNormalizedEquivalentQuestions() {
        QueryGenerationCacheService cacheService = cacheService(true, false, Map.of());
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        String firstKey = cacheService.buildExactCacheKey(" Show   all USERS ", strategy, "users(id);");
        String secondKey = cacheService.buildExactCacheKey("show all users", strategy, "users(id);");

        assertEquals(firstKey, secondKey);
    }

    @Test
    void skipsCacheWhenDisabled() {
        QueryGenerationCacheService cacheService = cacheService(false, false, Map.of());
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        cacheService.cacheQuery("show users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch cachedQuery = cacheService.findCachedQuery("show users", strategy, "users(id);");
        assertNull(cachedQuery);
    }

    @Test
    void reusesSemanticallySimilarPromptWhenThresholdMatches() {
        QueryGenerationCacheService cacheService = cacheService(
                true,
                true,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        cacheService.cacheQuery("show all users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch match = cacheService.findCachedQuery("list every user", strategy, "users(id);");

        assertNotNull(match);
        assertEquals(QueryGenerationCacheMatch.MatchType.SEMANTIC, match.matchType());
        assertEquals("SELECT * FROM users", match.query());
    }

    private QueryGenerationCacheService cacheService(
            boolean enabled,
            boolean semanticEnabled,
            Map<String, java.util.List<Float>> embeddings
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
                setStore.getOrDefault(invocation.getArgument(0), Set.of())
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

        EmbeddingService embeddingService = mock(EmbeddingService.class);
        embeddings.forEach((text, vector) -> when(embeddingService.embedText(text)).thenReturn(vector));

        return new QueryGenerationCacheService(
                configuration,
                redisTemplate,
                embeddingService,
                new ObjectMapper()
        );
    }

    private java.util.List<Float> vector(float... values) {
        java.util.List<Float> vector = new java.util.ArrayList<>();
        for (float value : values) {
            vector.add(value);
        }
        return vector;
    }
}
