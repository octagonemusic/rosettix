package com.rosettix.api.service;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.HexFormat;
import java.util.Locale;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueryGenerationCacheService {

    private static final String KEY_PREFIX = "rosettix:querygen:";

    private final RosettixConfiguration configuration;
    private final StringRedisTemplate redisTemplate;

    public String getCachedQuery(String question, QueryStrategy strategy, String schemaRepresentation) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        if (!queryConfig.isCachingEnabled()) {
            return null;
        }

        String key = buildCacheKey(question, strategy, schemaRepresentation);
        try {
            String cachedQuery = redisTemplate.opsForValue().get(key);
            if (cachedQuery != null) {
                log.info("Query generation cache hit for strategy {}", strategy.getStrategyName());
            }
            return cachedQuery;
        } catch (Exception e) {
            log.warn("Unable to read generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
            return null;
        }
    }

    public void cacheQuery(String question, QueryStrategy strategy, String schemaRepresentation, String cleanedQuery) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        if (!queryConfig.isCachingEnabled() || cleanedQuery == null || cleanedQuery.isBlank()) {
            return;
        }

        String key = buildCacheKey(question, strategy, schemaRepresentation);
        try {
            redisTemplate.opsForValue().set(
                    key,
                    cleanedQuery,
                    Duration.ofMinutes(queryConfig.getCacheTtlMinutes())
            );
            log.info("Stored generated query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            log.warn("Unable to write generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }
    }

    String buildCacheKey(String question, QueryStrategy strategy, String schemaRepresentation) {
        String normalizedQuestion = normalizeQuestion(question);
        String questionHash = sha256(normalizedQuestion);
        String schemaHash = sha256(schemaRepresentation == null ? "" : schemaRepresentation);
        return KEY_PREFIX + strategy.getStrategyName() + ":read:" + schemaHash + ":" + questionHash;
    }

    String normalizeQuestion(String question) {
        if (question == null) {
            return "";
        }

        return question
                .trim()
                .toLowerCase(Locale.ROOT)
                .replaceAll("\\s+", " ");
    }

    private String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm is not available", e);
        }
    }
}
