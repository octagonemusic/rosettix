package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class QueryGenerationCacheServiceTest {

    @Test
    void reusesSameExactCacheKeyForNormalizedEquivalentQuestions() {
        QueryGenerationCacheService cacheService = cacheService(true, false, null);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        String firstKey = cacheService.buildExactCacheKey(" Show   all USERS ", strategy, "users(id);");
        String secondKey = cacheService.buildExactCacheKey("show all users", strategy, "users(id);");

        assertEquals(firstKey, secondKey);
    }

    @Test
    void skipsCacheWhenDisabled() {
        QueryGenerationCacheService cacheService = cacheService(false, false, null);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        cacheService.cacheQuery("show users", strategy, "users(id);", "SELECT * FROM users");

        QueryGenerationCacheMatch cachedQuery = cacheService.findCachedQuery("show users", strategy, "users(id);");
        assertNull(cachedQuery);
    }

    @Test
    void reusesSemanticallySimilarPromptWhenThresholdMatches() {
        QueryGenerationCacheMatch semanticMatch = new QueryGenerationCacheMatch(
                "SELECT * FROM users",
                QueryGenerationCacheMatch.MatchType.SEMANTIC,
                0.9999
        );
        QueryGenerationCacheService cacheService = cacheService(true, true, semanticMatch);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        QueryGenerationCacheMatch match = cacheService.findCachedQuery("list every user", strategy, "users(id);");

        assertNotNull(match);
        assertEquals(QueryGenerationCacheMatch.MatchType.SEMANTIC, match.matchType());
        assertEquals("SELECT * FROM users", match.query());
    }

    @Test
    void storesSemanticEntryInRepositoryWhenEnabled() {
        SemanticQueryCacheRepository repository = mock(SemanticQueryCacheRepository.class);
        QueryGenerationCacheService cacheService = cacheService(true, true, null, repository);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        cacheService.cacheQuery("show users", strategy, "users(id);", "SELECT * FROM users");

        verify(repository).save(
                anyString(),
                anyString(),
                anyString(),
                anyString(),
                anyList()
        );
    }

    private QueryGenerationCacheService cacheService(
            boolean enabled,
            boolean semanticEnabled,
            QueryGenerationCacheMatch semanticMatch
    ) {
        return cacheService(enabled, semanticEnabled, semanticMatch, null);
    }

    private QueryGenerationCacheService cacheService(
            boolean enabled,
            boolean semanticEnabled,
            QueryGenerationCacheMatch semanticMatch,
            SemanticQueryCacheRepository customRepository
    ) {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getQuery().setCachingEnabled(enabled);
        configuration.getQuery().setCacheTtlMinutes(30);
        configuration.getQuery().setSemanticMatchingEnabled(semanticEnabled);
        configuration.getQuery().setSemanticThreshold(0.92);
        configuration.getQuery().setSemanticMaxCandidates(25);

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

        SemanticQueryCacheRepository repository = customRepository != null ? customRepository : mock(SemanticQueryCacheRepository.class);
        when(repository.findBestMatch(anyString(), anyString(), anyList(), anyDouble(), anyInt()))
                .thenReturn(semanticMatch);

        return new QueryGenerationCacheService(
                configuration,
                redisTemplate,
                embeddingService,
                repository
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
