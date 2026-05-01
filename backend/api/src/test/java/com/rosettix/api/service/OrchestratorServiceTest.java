package com.rosettix.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrchestratorServiceTest {

    @Test
    void reusesCachedGeneratedQueryForEquivalentReadPrompts() {
        RosettixConfiguration configuration = baseConfiguration();
        configuration.getQuery().setSemanticMatchingEnabled(false);

        LlmService llmService = mock(LlmService.class);
        QueryStrategy strategy = mockStrategy();

        when(llmService.generateQuery(" Show   all USERS ", strategy, "users(id, email);"))
                .thenReturn("SELECT id, email FROM users");

        QueryGenerationCacheService queryCacheService = queryCacheService(
                configuration,
                Map.of()
        );
        OrchestratorService orchestratorService = new OrchestratorService(
                llmService,
                queryCacheService,
                Map.of("postgres", strategy),
                configuration
        );

        List<Map<String, Object>> first = orchestratorService.processQuery(" Show   all USERS ", "postgres");
        List<Map<String, Object>> second = orchestratorService.processQuery("show all users", "postgres");

        assertEquals(first, second);
        verify(llmService, times(1)).generateQuery(" Show   all USERS ", strategy, "users(id, email);");
        verify(strategy, times(2)).executeQuery("SELECT id, email FROM users");
    }

    @Test
    void reusesSemanticallySimilarPromptWithoutCallingLlmAgain() {
        RosettixConfiguration configuration = baseConfiguration();
        configuration.getQuery().setSemanticMatchingEnabled(true);

        LlmService llmService = mock(LlmService.class);
        QueryStrategy strategy = mockStrategy();

        when(llmService.generateQuery("show all users", strategy, "users(id, email);"))
                .thenReturn("SELECT id, email FROM users");

        QueryGenerationCacheService queryCacheService = queryCacheService(
                configuration,
                Map.of(
                        "show all users", vector(1.0f, 0.0f),
                        "list every user", vector(0.99f, 0.01f)
                )
        );
        OrchestratorService orchestratorService = new OrchestratorService(
                llmService,
                queryCacheService,
                Map.of("postgres", strategy),
                configuration
        );

        List<Map<String, Object>> first = orchestratorService.processQuery("show all users", "postgres");
        List<Map<String, Object>> second = orchestratorService.processQuery("list every user", "postgres");

        assertEquals(first, second);
        verify(llmService, times(1)).generateQuery("show all users", strategy, "users(id, email);");
        verify(strategy, times(2)).executeQuery("SELECT id, email FROM users");
    }

    private RosettixConfiguration baseConfiguration() {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.setDefaultStrategy("postgres");
        configuration.getQuery().setCachingEnabled(true);
        configuration.getQuery().setCacheTtlMinutes(30);
        configuration.getQuery().setSemanticThreshold(0.92);
        configuration.getQuery().setSemanticMaxCandidates(25);
        return configuration;
    }

    private QueryStrategy mockStrategy() {
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");
        when(strategy.getSchemaRepresentation()).thenReturn("users(id, email);");
        when(strategy.cleanQuery(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        when(strategy.isQuerySafe(anyString())).thenReturn(true);
        when(strategy.isReadOperation(anyString())).thenReturn(true);
        when(strategy.executeQuery("SELECT id, email FROM users")).thenReturn(
                List.of(Map.of("id", 1, "email", "alice@example.com"))
        );
        return strategy;
    }

    private QueryGenerationCacheService queryCacheService(
            RosettixConfiguration configuration,
            Map<String, java.util.List<Float>> embeddings
    ) {
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
