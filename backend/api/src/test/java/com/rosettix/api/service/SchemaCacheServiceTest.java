package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCacheServiceTest {

    @Test
    void returnsCachedValueWithinTtl() {
        RosettixConfiguration configuration = configuration(true, 5);
        MutableClock clock = new MutableClock(Instant.parse("2026-03-27T10:00:00Z"));
        SchemaCacheService cacheService = new SchemaCacheService(configuration, clock);
        AtomicInteger loaderCalls = new AtomicInteger();

        String first = cacheService.getSchema("postgres", () -> {
            loaderCalls.incrementAndGet();
            return "schema-v1";
        });
        clock.advanceSeconds(30);
        String second = cacheService.getSchema("postgres", () -> {
            loaderCalls.incrementAndGet();
            return "schema-v2";
        });

        assertEquals("schema-v1", first);
        assertEquals("schema-v1", second);
        assertEquals(1, loaderCalls.get());
    }

    @Test
    void loadsSchemaOnFirstMiss() {
        RosettixConfiguration configuration = configuration(true, 5);
        MutableClock clock = new MutableClock(Instant.parse("2026-03-27T10:00:00Z"));
        SchemaCacheService cacheService = new SchemaCacheService(configuration, clock);
        AtomicInteger loaderCalls = new AtomicInteger();

        String schema = cacheService.getSchema("mongodb", () -> {
            loaderCalls.incrementAndGet();
            return "mongo-schema";
        });

        assertEquals("mongo-schema", schema);
        assertEquals(1, loaderCalls.get());
    }

    @Test
    void reloadsSchemaAfterExpiry() {
        RosettixConfiguration configuration = configuration(true, 5);
        MutableClock clock = new MutableClock(Instant.parse("2026-03-27T10:00:00Z"));
        SchemaCacheService cacheService = new SchemaCacheService(configuration, clock);
        AtomicInteger loaderCalls = new AtomicInteger();

        String first = cacheService.getSchema("postgres", () -> {
            loaderCalls.incrementAndGet();
            return "schema-v1";
        });
        clock.advanceSeconds(301);
        String second = cacheService.getSchema("postgres", () -> {
            loaderCalls.incrementAndGet();
            return "schema-v2";
        });

        assertEquals("schema-v1", first);
        assertEquals("schema-v2", second);
        assertEquals(2, loaderCalls.get());
    }

    @Test
    void bypassesCacheWhenDisabled() {
        RosettixConfiguration configuration = configuration(false, 5);
        MutableClock clock = new MutableClock(Instant.parse("2026-03-27T10:00:00Z"));
        SchemaCacheService cacheService = new SchemaCacheService(configuration, clock);
        AtomicInteger loaderCalls = new AtomicInteger();

        String first = cacheService.getSchema("mongodb", () -> {
            int count = loaderCalls.incrementAndGet();
            return "schema-v" + count;
        });
        String second = cacheService.getSchema("mongodb", () -> {
            int count = loaderCalls.incrementAndGet();
            return "schema-v" + count;
        });

        assertEquals("schema-v1", first);
        assertEquals("schema-v2", second);
        assertEquals(2, loaderCalls.get());
    }

    @Test
    @SuppressWarnings("unchecked")
    void reportsLatencyReductionMetrics() {
        RosettixConfiguration configuration = configuration(true, 5);
        MutableClock clock = new MutableClock(Instant.parse("2026-03-27T10:00:00Z"));
        SchemaCacheService cacheService = new SchemaCacheService(configuration, clock);

        cacheService.getSchema("postgres", () -> {
            sleep(30);
            return "schema-v1";
        });
        cacheService.getSchema("postgres", () -> {
            sleep(30);
            return "schema-v2";
        });

        Map<String, Object> snapshot = cacheService.getMetricsSnapshot();
        Map<String, Object> databases = (Map<String, Object>) snapshot.get("databases");
        Map<String, Object> postgres = (Map<String, Object>) databases.get("postgres");

        assertNotNull(postgres);
        assertEquals(1L, postgres.get("cache_hits"));
        assertEquals(1L, postgres.get("cache_misses"));
        assertTrue((Double) postgres.get("avg_miss_ms") > 0.0);
        assertTrue((Double) postgres.get("avg_hit_ms") >= 0.0);
        assertNotNull(postgres.get("estimated_latency_reduction_percent"));
    }

    private RosettixConfiguration configuration(boolean enabled, long ttlMinutes) {
        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getSchemaCache().setEnabled(enabled);
        configuration.getSchemaCache().setTtlMinutes(ttlMinutes);
        return configuration;
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while simulating schema fetch", e);
        }
    }

    private static final class MutableClock extends Clock {
        private Instant instant;

        private MutableClock(Instant instant) {
            this.instant = instant;
        }

        @Override
        public ZoneOffset getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(java.time.ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }

        private void advanceSeconds(long seconds) {
            instant = instant.plusSeconds(seconds);
        }
    }
}
