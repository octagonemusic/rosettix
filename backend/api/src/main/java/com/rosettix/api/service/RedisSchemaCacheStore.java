package com.rosettix.api.service;

import lombok.RequiredArgsConstructor;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@RequiredArgsConstructor
public class RedisSchemaCacheStore implements SchemaCacheStore {

    private static final String KEY_PREFIX = "rosettix:schema:";

    private final StringRedisTemplate redisTemplate;

    @Override
    public String get(String key) {
        return redisTemplate.opsForValue().get(buildKey(key));
    }

    @Override
    public void put(String key, String schema, Duration ttl) {
        redisTemplate.opsForValue().set(buildKey(key), schema, ttl);
    }

    @Override
    public String getStoreType() {
        return "redis";
    }

    private String buildKey(String key) {
        return KEY_PREFIX + key;
    }
}
