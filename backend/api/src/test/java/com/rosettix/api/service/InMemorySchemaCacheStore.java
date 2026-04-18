package com.rosettix.api.service;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class InMemorySchemaCacheStore implements SchemaCacheStore {

    private final Clock clock;
    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    public InMemorySchemaCacheStore(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String get(String key) {
        CacheEntry entry = cache.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.expiresAt().isBefore(Instant.now(clock))) {
            cache.remove(key);
            return null;
        }

        return entry.schema();
    }

    @Override
    public void put(String key, String schema, Duration ttl) {
        cache.put(key, new CacheEntry(schema, Instant.now(clock).plus(ttl)));
    }

    @Override
    public String getStoreType() {
        return "in-memory-test";
    }

    private record CacheEntry(String schema, Instant expiresAt) {
    }
}
