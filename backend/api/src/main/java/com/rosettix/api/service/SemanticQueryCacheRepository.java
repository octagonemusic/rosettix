package com.rosettix.api.service;

import java.util.List;

public interface SemanticQueryCacheRepository {

    QueryGenerationCacheMatch findBestMatch(
            String strategy,
            String schemaHash,
            List<Float> embedding,
            double similarityThreshold,
            int limit
    );

    void save(
            String strategy,
            String schemaHash,
            String normalizedQuestion,
            String generatedQuery,
            List<Float> embedding
    );
}
