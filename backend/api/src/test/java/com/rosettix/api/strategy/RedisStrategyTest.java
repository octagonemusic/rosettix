package com.rosettix.api.strategy;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.service.InMemorySchemaCacheStore;
import com.rosettix.api.service.SchemaCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ValueOperations;
import org.springframework.data.redis.core.ZSetOperations;

import java.time.Clock;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RedisStrategyTest {

    @Test
    void reusesCachedSchemaWithinTtl() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);

        when(redisTemplate.keys("*")).thenReturn(new LinkedHashSet<>(Set.of("users:1")));
        when(redisTemplate.type("users:1")).thenReturn(DataType.STRING);
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(valueOperations.get("users:1")).thenReturn("Alice");

        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getSchemaCache().setEnabled(true);
        configuration.getSchemaCache().setTtlMinutes(5);
        SchemaCacheService schemaCacheService = new SchemaCacheService(
                configuration,
                new InMemorySchemaCacheStore(Clock.systemUTC())
        );

        RedisStrategy strategy = new RedisStrategy(redisTemplate, schemaCacheService);

        String first = strategy.getSchemaRepresentation();
        String second = strategy.getSchemaRepresentation();

        assertEquals("users:1(string): value=\"Alice\"; ", first);
        assertEquals(first, second);
        verify(redisTemplate, times(1)).keys("*");
    }

    @Test
    void executesReadCommands() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        HashOperations<String, Object, Object> hashOperations = mock(HashOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForHash()).thenReturn(hashOperations);
        when(valueOperations.get("session:1")).thenReturn("active");
        when(hashOperations.get("user:1", "email")).thenReturn("alice@example.com");

        RedisStrategy strategy = new RedisStrategy(redisTemplate, cacheService());

        List<Map<String, Object>> getResult = strategy.executeQuery("GET session:1");
        List<Map<String, Object>> hgetResult = strategy.executeQuery("HGET user:1 email");

        assertEquals("active", getResult.get(0).get("value"));
        assertEquals("alice@example.com", hgetResult.get(0).get("value"));
        assertTrue(strategy.isReadOperation("GET session:1"));
        assertFalse(strategy.isWriteOperation("GET session:1"));
    }

    @Test
    void executesWriteCommands() {
        StringRedisTemplate redisTemplate = mock(StringRedisTemplate.class);
        ValueOperations<String, String> valueOperations = mock(ValueOperations.class);
        ListOperations<String, String> listOperations = mock(ListOperations.class);
        SetOperations<String, String> setOperations = mock(SetOperations.class);
        ZSetOperations<String, String> zSetOperations = mock(ZSetOperations.class);

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        when(redisTemplate.opsForList()).thenReturn(listOperations);
        when(redisTemplate.opsForSet()).thenReturn(setOperations);
        when(redisTemplate.opsForZSet()).thenReturn(zSetOperations);
        when(listOperations.rightPush("jobs", "task-1")).thenReturn(1L);
        when(setOperations.add("roles", "admin")).thenReturn(1L);
        when(zSetOperations.add("leaders", "alice", 42.5)).thenReturn(true);

        RedisStrategy strategy = new RedisStrategy(redisTemplate, cacheService());

        List<Map<String, Object>> setResult = strategy.executeQuery("SET user:1 Alice");
        List<Map<String, Object>> rpushResult = strategy.executeQuery("RPUSH jobs task-1");
        List<Map<String, Object>> saddResult = strategy.executeQuery("SADD roles admin");
        List<Map<String, Object>> zaddResult = strategy.executeQuery("ZADD leaders 42.5 alice");

        assertEquals("OK", setResult.get(0).get("status"));
        assertEquals(1L, rpushResult.get(0).get("size"));
        assertEquals(1L, saddResult.get(0).get("added"));
        assertEquals(true, zaddResult.get(0).get("added"));
        assertTrue(strategy.isWriteOperation("SET user:1 Alice"));
        assertFalse(strategy.isReadOperation("SET user:1 Alice"));
        verify(valueOperations).set("user:1", "Alice");
    }

    @Test
    void blocksAccessToReservedCacheKeys() {
        RedisStrategy strategy = new RedisStrategy(mock(StringRedisTemplate.class), cacheService());

        assertFalse(strategy.isQuerySafe("GET rosettix:schema:postgres"));
        assertThrows(SecurityException.class, () -> strategy.executeQuery("GET rosettix:schema:postgres"));
    }

    private SchemaCacheService cacheService() {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getSchemaCache().setEnabled(true);
        configuration.getSchemaCache().setTtlMinutes(5);
        return new SchemaCacheService(configuration, new InMemorySchemaCacheStore(Clock.systemUTC()));
    }
}
