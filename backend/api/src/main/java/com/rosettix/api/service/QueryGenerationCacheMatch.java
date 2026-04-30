package com.rosettix.api.service;

public record QueryGenerationCacheMatch(
        String query,
        MatchType matchType,
        double similarityScore
) {
    public enum MatchType {
        EXACT,
        SEMANTIC
    }
}
