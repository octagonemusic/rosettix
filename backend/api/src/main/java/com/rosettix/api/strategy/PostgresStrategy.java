package com.rosettix.api.strategy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("postgres") // We give it a name to find it later
@RequiredArgsConstructor
public class PostgresStrategy implements QueryStrategy {

    private final JdbcTemplate jdbcTemplate;

    @Override
    public String getSchemaRepresentation() {
        String sql =
            "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' ORDER BY table_name, ordinal_position;";
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

        Map<String, List<String>> tables = rows
            .stream()
            .collect(
                Collectors.groupingBy(
                    row -> (String) row.get("table_name"),
                    Collectors.mapping(
                        row -> (String) row.get("column_name"),
                        Collectors.toList()
                    )
                )
            );

        StringBuilder schemaBuilder = new StringBuilder();
        tables.forEach((tableName, columns) -> {
            schemaBuilder
                .append(tableName)
                .append("(")
                .append(String.join(", ", columns))
                .append("); ");
        });
        return schemaBuilder.toString();
    }

    @Override
    public String getQueryLanguage() {
        return "PostgreSQL";
    }

    @Override
    public String getStrategyName() {
        return "postgres";
    }

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.trim().isEmpty()) {
            return false;
        }

        // Basic SQL injection protection - prevent dangerous operations
        String lowerQuery = query.toLowerCase().trim();
        String[] dangerousKeywords = {
            "drop",
            "delete",
            "truncate",
            "alter",
            "create",
            "insert",
            "update",
        };

        for (String keyword : dangerousKeywords) {
            if (
                lowerQuery.startsWith(keyword + " ") ||
                lowerQuery.contains(";" + keyword + " ")
            ) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String buildPrompt(String question, String schema) {
        return String.format(
            "Given the PostgreSQL schema: \n%s\n---\nTranslate the question into a single, valid PostgreSQL SELECT query. " +
                "Do not add any explanation, comments, or markdown formatting. " +
                "Only return the SQL query.\nQuestion: \"%s\"",
            schema,
            question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        if (rawQuery == null) {
            return "";
        }

        // Remove common markdown code block delimiters
        String cleaned = rawQuery.replace("```sql", "").replace("```", "");

        // Trim whitespace and remove trailing semicolons
        return cleaned.trim().replaceAll(";+\\s*$", "");
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        return jdbcTemplate.queryForList(query);
    }
}
