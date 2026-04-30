package com.rosettix.api.service;

import java.util.List;

public record SemanticQueryCacheEntry(
        String strategy,
        String schemaHash,
        String normalizedQuestion,
        String generatedQuery,
        List<Float> embedding,
        String cachedAt
) {
}
