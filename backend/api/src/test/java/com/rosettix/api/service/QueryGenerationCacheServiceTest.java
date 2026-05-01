package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryGenerationCacheServiceTest {

    @Test
    void reusesSameCacheKeyForNormalizedEquivalentQuestions() {
        QueryGenerationCacheService cacheService = cacheService(true);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        String firstKey = cacheService.buildCacheKey(" Show   all USERS ", strategy, "users(id);");
        String secondKey = cacheService.buildCacheKey("show all users", strategy, "users(id);");

        assertEquals(firstKey, secondKey);
    }

    @Test
    void skipsCacheWhenDisabled() {
        QueryGenerationCacheService cacheService = cacheService(false);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");

        cacheService.cacheQuery("show users", strategy, "users(id);", "SELECT * FROM users");

        String cachedQuery = cacheService.getCachedQuery("show users", strategy, "users(id);");
        assertNull(cachedQuery);
    }

    private QueryGenerationCacheService cacheService(boolean enabled) {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getQuery().setCachingEnabled(enabled);
        configuration.getQuery().setCacheTtlMinutes(30);

        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        Map<String, String> store = new ConcurrentHashMap<>();

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get(anyString())).thenAnswer(invocation -> store.get(invocation.getArgument(0)));
        doAnswer(invocation -> {
            store.put(invocation.getArgument(0), invocation.getArgument(1));
            return null;
        }).when(valueOperations).set(anyString(), anyString(), any());

        return new QueryGenerationCacheService(configuration, redisTemplate);
    }
}
