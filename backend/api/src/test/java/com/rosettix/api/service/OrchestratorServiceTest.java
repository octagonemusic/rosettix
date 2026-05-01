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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OrchestratorServiceTest {

    @Test
    void reusesCachedGeneratedQueryForEquivalentReadPrompts() {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.setDefaultStrategy("postgres");
        configuration.getQuery().setCachingEnabled(true);
        configuration.getQuery().setCacheTtlMinutes(30);

        LlmService llmService = mock(LlmService.class);
        QueryStrategy strategy = mock(QueryStrategy.class);
        when(strategy.getStrategyName()).thenReturn("postgres");
        when(strategy.getSchemaRepresentation()).thenReturn("users(id, email);");
        when(strategy.cleanQuery(anyString())).thenAnswer(invocation -> invocation.getArgument(0));
        when(strategy.isQuerySafe(anyString())).thenReturn(true);
        when(strategy.isReadOperation(anyString())).thenReturn(true);
        when(strategy.executeQuery("SELECT id, email FROM users")).thenReturn(
                List.of(Map.of("id", 1, "email", "alice@example.com"))
        );

        when(llmService.generateQuery(" Show   all USERS ", strategy, "users(id, email);"))
                .thenReturn("SELECT id, email FROM users");

        QueryGenerationCacheService queryCacheService = queryCacheService(configuration);
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

    private QueryGenerationCacheService queryCacheService(RosettixConfiguration configuration) {
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
