package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
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

        QueryGenerationCacheService queryCacheService = queryCacheService(configuration, null);
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

        QueryGenerationCacheMatch semanticMatch = new QueryGenerationCacheMatch(
                "SELECT id, email FROM users",
                QueryGenerationCacheMatch.MatchType.SEMANTIC,
                0.9999
        );
        QueryGenerationCacheService queryCacheService = queryCacheService(configuration, null, semanticMatch);
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
            QueryGenerationCacheMatch firstSemanticMatch,
            QueryGenerationCacheMatch secondSemanticMatch
    ) {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        Map<String, String> valueStore = new ConcurrentHashMap<>();

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(anyString())).thenAnswer(invocation -> valueStore.get(invocation.getArgument(0)));
        doAnswer(invocation -> {
            valueStore.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(valueOperations).set(anyString(), anyString(), any());

        EmbeddingService embeddingService = mock(EmbeddingService.class);
        when(embeddingService.embedText(anyString())).thenReturn(vector(1.0f, 0.0f));

        SemanticQueryCacheRepository repository = mock(SemanticQueryCacheRepository.class);
        when(repository.findBestMatch(anyString(), anyString(), anyList(), anyDouble(), anyInt()))
                .thenReturn(firstSemanticMatch, secondSemanticMatch);

        return new QueryGenerationCacheService(
                configuration,
                redisTemplate,
                embeddingService,
                repository
        );
    }

    private QueryGenerationCacheService queryCacheService(
            RosettixConfiguration configuration,
            QueryGenerationCacheMatch semanticMatch
    ) {
        return queryCacheService(configuration, semanticMatch, semanticMatch);
    }

    private java.util.List<Float> vector(float... values) {
        java.util.List<Float> vector = new java.util.ArrayList<>();
        for (float value : values) {
            vector.add(value);
        }
        return vector;
    }
}
