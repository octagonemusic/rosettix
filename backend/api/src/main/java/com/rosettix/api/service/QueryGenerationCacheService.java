package com.rosettix.api.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.strategy.QueryStrategy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueryGenerationCacheService {

    static final String KEY_PREFIX = "rosettix:querygen:";

    private final RosettixConfiguration configuration;
    private final StringRedisTemplate redisTemplate;
    private final EmbeddingService embeddingService;
    private final ObjectMapper objectMapper;

    public QueryGenerationCacheMatch findCachedQuery(String question, QueryStrategy strategy, String schemaRepresentation) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        if (!queryConfig.isCachingEnabled()) {
            return null;
        }

        String key = buildExactCacheKey(question, strategy, schemaRepresentation);
        try {
            String cachedQuery = redisTemplate.opsForValue().get(key);
            if (cachedQuery != null) {
                log.info("Query generation exact cache hit for strategy {}", strategy.getStrategyName());
                return new QueryGenerationCacheMatch(
                        cachedQuery,
                        QueryGenerationCacheMatch.MatchType.EXACT,
                        1.0
                );
            }
        } catch (Exception e) {
            log.warn("Unable to read generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }

        if (!queryConfig.isSemanticMatchingEnabled()) {
            return null;
        }

        return findSemanticMatch(question, strategy, schemaRepresentation);
    }

    public void cacheQuery(String question, QueryStrategy strategy, String schemaRepresentation, String cleanedQuery) {
        RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
        if (!queryConfig.isCachingEnabled() || cleanedQuery == null || cleanedQuery.isBlank()) {
            return;
        }

        String schemaHash = schemaHash(schemaRepresentation);
        String key = buildExactCacheKey(question, strategy, schemaRepresentation);
        Duration ttl = Duration.ofMinutes(queryConfig.getCacheTtlMinutes());

        try {
            redisTemplate.opsForValue().set(key, cleanedQuery, ttl);
            log.info("Stored generated query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            log.warn("Unable to write generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }

        if (queryConfig.isSemanticMatchingEnabled()) {
            storeSemanticEntry(question, strategy, schemaHash, cleanedQuery, ttl);
        }
    }

    String buildExactCacheKey(String question, QueryStrategy strategy, String schemaRepresentation) {
        String normalizedQuestion = normalizeQuestion(question);
        String questionHash = sha256(normalizedQuestion);
        String schemaHash = schemaHash(schemaRepresentation);
        return KEY_PREFIX + strategy.getStrategyName() + ":read:" + schemaHash + ":" + questionHash;
    }

    String buildSemanticEntryKey(String question, QueryStrategy strategy, String schemaHash) {
        String questionHash = sha256(normalizeQuestion(question));
        return KEY_PREFIX + strategy.getStrategyName() + ":semantic:read:" + schemaHash + ":" + questionHash;
    }

    String buildSemanticIndexKey(QueryStrategy strategy, String schemaHash) {
        return KEY_PREFIX + strategy.getStrategyName() + ":semantic:index:read:" + schemaHash;
    }

    String schemaHash(String schemaRepresentation) {
        return sha256(schemaRepresentation == null ? "" : schemaRepresentation);
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

    private QueryGenerationCacheMatch findSemanticMatch(String question, QueryStrategy strategy, String schemaRepresentation) {
        try {
            RosettixConfiguration.QueryConfig queryConfig = configuration.getQuery();
            String schemaHash = schemaHash(schemaRepresentation);
            String normalizedQuestion = normalizeQuestion(question);
            List<Float> inputEmbedding = embeddingService.embedText(normalizedQuestion);
            Set<String> entryKeys = redisTemplate.opsForSet().members(buildSemanticIndexKey(strategy, schemaHash));
            if (entryKeys == null || entryKeys.isEmpty()) {
                return null;
            }

            QueryGenerationCacheMatch bestMatch = null;
            double bestScore = -1.0;
            int processed = 0;
            for (String entryKey : new LinkedHashSet<>(entryKeys)) {
                if (processed >= queryConfig.getSemanticMaxCandidates()) {
                    break;
                }
                processed++;

                String payload = redisTemplate.opsForValue().get(entryKey);
                if (payload == null) {
                    safeRemoveIndexEntry(buildSemanticIndexKey(strategy, schemaHash), entryKey);
                    continue;
                }

                SemanticQueryCacheEntry entry = objectMapper.readValue(payload, SemanticQueryCacheEntry.class);
                double similarity = cosineSimilarity(inputEmbedding, entry.embedding());
                if (similarity >= queryConfig.getSemanticThreshold() && similarity > bestScore) {
                    bestScore = similarity;
                    bestMatch = new QueryGenerationCacheMatch(
                            entry.generatedQuery(),
                            QueryGenerationCacheMatch.MatchType.SEMANTIC,
                            similarity
                    );
                }
            }

            if (bestMatch != null) {
                log.info(
                        "Query generation semantic cache hit for strategy {} with similarity {}",
                        strategy.getStrategyName(),
                        String.format("%.4f", bestMatch.similarityScore())
                );
            }
            return bestMatch;
        } catch (Exception e) {
            log.warn("Semantic query cache lookup failed for {}: {}", strategy.getStrategyName(), e.getMessage());
            return null;
        }
    }

    private void storeSemanticEntry(
            String question,
            QueryStrategy strategy,
            String schemaHash,
            String cleanedQuery,
            Duration ttl
    ) {
        try {
            String normalizedQuestion = normalizeQuestion(question);
            SemanticQueryCacheEntry entry = new SemanticQueryCacheEntry(
                    strategy.getStrategyName(),
                    schemaHash,
                    normalizedQuestion,
                    cleanedQuery,
                    embeddingService.embedText(normalizedQuestion),
                    Instant.now().toString()
            );
            String entryKey = buildSemanticEntryKey(question, strategy, schemaHash);
            redisTemplate.opsForValue().set(entryKey, objectMapper.writeValueAsString(entry), ttl);

            String indexKey = buildSemanticIndexKey(strategy, schemaHash);
            SetOperations<String, String> setOperations = redisTemplate.opsForSet();
            setOperations.add(indexKey, entryKey);
            redisTemplate.expire(indexKey, ttl);
            log.info("Stored semantic query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            log.warn("Unable to write semantic query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }
    }

    private void safeRemoveIndexEntry(String indexKey, String entryKey) {
        try {
            redisTemplate.opsForSet().remove(indexKey, entryKey);
        } catch (Exception e) {
            log.debug("Unable to remove stale semantic cache index entry {}: {}", entryKey, e.getMessage());
        }
    }

    private double cosineSimilarity(List<Float> first, List<Float> second) {
        if (first == null || second == null || first.isEmpty() || second.isEmpty()) {
            return -1.0;
        }

        int size = Math.min(first.size(), second.size());
        double dotProduct = 0.0;
        double firstMagnitude = 0.0;
        double secondMagnitude = 0.0;

        for (int i = 0; i < size; i++) {
            double left = first.get(i);
            double right = second.get(i);
            dotProduct += left * right;
            firstMagnitude += left * left;
            secondMagnitude += right * right;
        }

        if (firstMagnitude == 0.0 || secondMagnitude == 0.0) {
            return -1.0;
        }

        return dotProduct / (Math.sqrt(firstMagnitude) * Math.sqrt(secondMagnitude));
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
