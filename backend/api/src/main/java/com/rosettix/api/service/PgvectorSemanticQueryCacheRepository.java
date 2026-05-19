package com.rosettix.api.service;

import com.pgvector.PGvector;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

@Repository
@RequiredArgsConstructor
@Slf4j
public class PgvectorSemanticQueryCacheRepository implements SemanticQueryCacheRepository {

    private final JdbcTemplate jdbcTemplate;
    private final DataSource dataSource;

    @Override
    public QueryGenerationCacheMatch findBestMatch(
            String strategy,
            String schemaHash,
            List<Float> embedding,
            double similarityThreshold,
            int limit
    ) {
        float[] vector = toFloatArray(embedding);
        return jdbcTemplate.execute((Connection connection) -> {
            PGvector.registerTypes(connection);
            String sql = """
                    SELECT generated_query,
                           1 - (embedding <=> ?) AS cosine_similarity
                    FROM semantic_query_cache
                    WHERE strategy = ?
                      AND schema_hash = ?
                    ORDER BY embedding <=> ?
                    LIMIT ?
                    """;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setObject(1, new PGvector(vector));
                statement.setString(2, strategy);
                statement.setString(3, schemaHash);
                statement.setObject(4, new PGvector(vector));
                statement.setInt(5, limit);

                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        return null;
                    }

                    double similarity = resultSet.getDouble("cosine_similarity");
                    if (similarity < similarityThreshold) {
                        return null;
                    }

                    return new QueryGenerationCacheMatch(
                            resultSet.getString("generated_query"),
                            QueryGenerationCacheMatch.MatchType.SEMANTIC,
                            similarity
                    );
                }
            }
        });
    }

    @Override
    public void save(
            String strategy,
            String schemaHash,
            String normalizedQuestion,
            String generatedQuery,
            List<Float> embedding
    ) {
        float[] vector = toFloatArray(embedding);
        jdbcTemplate.execute((Connection connection) -> {
            PGvector.registerTypes(connection);
            String sql = """
                    INSERT INTO semantic_query_cache (
                        strategy,
                        schema_hash,
                        normalized_question,
                        generated_query,
                        successful_executions,
                        embedding
                    ) VALUES (?, ?, ?, ?, 1, ?)
                    ON CONFLICT (strategy, schema_hash, normalized_question)
                    DO UPDATE SET
                        generated_query = EXCLUDED.generated_query,
                        embedding = EXCLUDED.embedding,
                        successful_executions = semantic_query_cache.successful_executions + 1,
                        updated_at = now()
                    """;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, strategy);
                statement.setString(2, schemaHash);
                statement.setString(3, normalizedQuestion);
                statement.setString(4, generatedQuery);
                statement.setObject(5, new PGvector(vector));
                statement.executeUpdate();
                return null;
            }
        });
    }

    public void initializeSchema(int embeddingDimensions) {
        jdbcTemplate.execute("CREATE EXTENSION IF NOT EXISTS vector");
        jdbcTemplate.execute("""
                CREATE TABLE IF NOT EXISTS semantic_query_cache (
                    id BIGSERIAL PRIMARY KEY,
                    strategy VARCHAR(64) NOT NULL,
                    schema_hash VARCHAR(64) NOT NULL,
                    normalized_question TEXT NOT NULL,
                    generated_query TEXT NOT NULL,
                    successful_executions INTEGER NOT NULL DEFAULT 1,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    embedding vector(%d) NOT NULL,
                    CONSTRAINT semantic_query_cache_unique_prompt
                        UNIQUE (strategy, schema_hash, normalized_question)
                )
                """.formatted(embeddingDimensions));
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS semantic_query_cache_strategy_schema_idx
                ON semantic_query_cache (strategy, schema_hash)
                """);
        jdbcTemplate.execute("""
                CREATE INDEX IF NOT EXISTS semantic_query_cache_embedding_hnsw_idx
                ON semantic_query_cache
                USING hnsw (embedding vector_cosine_ops)
                """);
    }

    private float[] toFloatArray(List<Float> embedding) {
        float[] values = new float[embedding.size()];
        for (int i = 0; i < embedding.size(); i++) {
            values[i] = embedding.get(i);
        }
        return values;
    }
}
