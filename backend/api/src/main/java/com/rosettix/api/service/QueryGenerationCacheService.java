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
import java.util.List;
import java.util.Locale;

@Service
@RequiredArgsConstructor
@Slf4j
public class QueryGenerationCacheService {

    static final String KEY_PREFIX = "rosettix:querygen:";

    private final RosettixConfiguration configuration;
    private final StringRedisTemplate redisTemplate;
    private final EmbeddingService embeddingService;
    private final SemanticQueryCacheRepository semanticQueryCacheRepository;

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
        String normalizedQuestion = normalizeQuestion(question);

        try {
            redisTemplate.opsForValue().set(key, cleanedQuery, ttl);
            log.info("Stored generated query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            log.warn("Unable to write generated query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }

        if (queryConfig.isSemanticMatchingEnabled()) {
            storeSemanticEntry(strategy, schemaHash, normalizedQuestion, cleanedQuery);
        }
    }

    String buildExactCacheKey(String question, QueryStrategy strategy, String schemaRepresentation) {
        String normalizedQuestion = normalizeQuestion(question);
        String questionHash = sha256(normalizedQuestion);
        String schemaHash = schemaHash(schemaRepresentation);
        return KEY_PREFIX + strategy.getStrategyName() + ":read:" + schemaHash + ":" + questionHash;
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
            QueryGenerationCacheMatch bestMatch = semanticQueryCacheRepository.findBestMatch(
                    strategy.getStrategyName(),
                    schemaHash,
                    inputEmbedding,
                    queryConfig.getSemanticThreshold(),
                    queryConfig.getSemanticMaxCandidates()
            );
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
            QueryStrategy strategy,
            String schemaHash,
            String normalizedQuestion,
            String cleanedQuery
    ) {
        try {
            semanticQueryCacheRepository.save(
                    strategy.getStrategyName(),
                    schemaHash,
                    normalizedQuestion,
                    cleanedQuery,
                    embeddingService.embedText(normalizedQuestion)
            );
            log.info("Stored semantic query cache entry for strategy {}", strategy.getStrategyName());
        } catch (Exception e) {
            log.warn("Unable to write semantic query cache entry for {}: {}", strategy.getStrategyName(), e.getMessage());
        }
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
