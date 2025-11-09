package com.rosettix.api.strategy;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("postgres")
@RequiredArgsConstructor
@Slf4j
public class PostgresStrategy implements QueryStrategy {

    private final JdbcTemplate jdbcTemplate;

    // ============================================================
    // 1️⃣ SCHEMA AND INTROSPECTION
    // ============================================================
    @Override
    public String getSchemaRepresentation() {
        try {
            String sql = """
                SELECT table_name, column_name, data_type 
                FROM information_schema.columns 
                WHERE table_schema = 'public' 
                ORDER BY table_name, ordinal_position;
            """;

            List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

            Map<String, List<String>> tables = rows.stream()
                    .collect(Collectors.groupingBy(
                            row -> (String) row.get("table_name"),
                            Collectors.mapping(row -> (String) row.get("column_name"), Collectors.toList())
                    ));

            StringBuilder schemaBuilder = new StringBuilder();
            tables.forEach((tableName, columns) -> schemaBuilder
                    .append(tableName)
                    .append("(")
                    .append(String.join(", ", columns))
                    .append("); "));

            return schemaBuilder.toString();
        } catch (Exception e) {
            log.error("Error fetching schema: {}", e.getMessage(), e);
            return "Error retrieving PostgreSQL schema.";
        }
    }

    @Override
    public String getQueryLanguage() {
        return "PostgreSQL";
    }

    @Override
    public String getStrategyName() {
        return "postgres";
    }

    // ============================================================
    // 2️⃣ AI PROMPT GENERATION
    // ============================================================
    @Override
    public String buildPrompt(String question, String schema) {
        return String.format(
            "Given the PostgreSQL schema: \n%s\n---\n" +
            "Translate the question into a valid SQL query. " +
            "For read queries, use SELECT. " +
            "If the intent is to insert, update, or delete data, generate a safe DML statement. " +
            "Avoid destructive operations like DROP, TRUNCATE, or ALTER.\n" +
            "Return only the SQL query (no markdown or explanation).\nQuestion: \"%s\"",
            schema, question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        if (rawQuery == null) return "";
        String cleaned = rawQuery
                .replace("```sql", "")
                .replace("```postgresql", "")
                .replace("```", "")
                .trim();
        return cleaned.replaceAll(";+\\s*$", "");
    }

    // ============================================================
    // 3️⃣ QUERY SAFETY GUARD
    // ============================================================
    private static final Pattern UNSAFE_PATTERN = Pattern.compile(
        "\\b(drop|truncate|alter|grant|revoke|create|replace|exec|execute)\\b",
        Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.isBlank()) return false;
        String lower = query.toLowerCase(Locale.ROOT);
        return !lower.contains(";")
                && !lower.contains("--")
                && !UNSAFE_PATTERN.matcher(lower).find();
    }

    // ============================================================
    // 4️⃣ QUERY EXECUTION (AI-GENERATED, SAFE HANDLER)
    // ============================================================
    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }

        String lower = query.trim().toLowerCase(Locale.ROOT);
        if (!isQuerySafe(lower)) {
            throw new SecurityException("Blocked potentially unsafe SQL operation.");
        }

        try {
            if (lower.startsWith("select")) {
                log.info("Executing safe SELECT query.");
                return jdbcTemplate.queryForList(query);
            } else if (lower.startsWith("insert") ||
                       lower.startsWith("update") ||
                       lower.startsWith("delete")) {

                int affected = jdbcTemplate.update(query);
                log.info("Executed DML query, affected rows: {}", affected);
                return List.of(Map.of("rows_affected", affected));
            } else {
                throw new IllegalArgumentException("Unsupported SQL command: Only SELECT/INSERT/UPDATE/DELETE allowed.");
            }
        } catch (DataAccessException e) {
            log.error("SQL execution error: {}", e.getMessage(), e);
            throw new RuntimeException("SQL Execution Error: " + e.getMessage(), e);
        }
    }

    // ============================================================
    // 5️⃣ AI-BASED SAFE WRITES (FOR SAGA)
    // ============================================================
    public int insert(String table, Map<String, Object> values) {
        if (values == null || values.isEmpty())
            throw new IllegalArgumentException("values cannot be empty");

        String columns = String.join(", ", values.keySet());
        String valueLiterals = values.values().stream()
                .map(v -> v instanceof String ? "'" + v + "'" : v.toString())
                .collect(Collectors.joining(", "));

        String query = String.format("INSERT INTO %s (%s) VALUES (%s)", table, columns, valueLiterals);
        List<Map<String, Object>> result = executeQuery(query);
        Object rows = result.get(0).get("rows_affected");
        return rows == null ? 0 : ((Number) rows).intValue();
    }

    public int update(String table, Map<String, Object> setValues, String whereClause) {
        if (setValues == null || setValues.isEmpty())
            throw new IllegalArgumentException("setValues cannot be empty");

        String setSql = setValues.entrySet().stream()
                .map(e -> e.getValue() instanceof String
                        ? e.getKey() + " = '" + e.getValue() + "'"
                        : e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining(", "));

        String query = String.format("UPDATE %s SET %s%s",
                table, setSql,
                (whereClause == null || whereClause.isBlank()) ? "" : " WHERE " + whereClause);

        List<Map<String, Object>> result = executeQuery(query);
        Object rows = result.get(0).get("rows_affected");
        return rows == null ? 0 : ((Number) rows).intValue();
    }

    public int delete(String table, String whereClause) {
        String query = String.format("DELETE FROM %s%s",
                table,
                (whereClause == null || whereClause.isBlank()) ? "" : " WHERE " + whereClause);

        List<Map<String, Object>> result = executeQuery(query);
        Object rows = result.get(0).get("rows_affected");
        return rows == null ? 0 : ((Number) rows).intValue();
    }

    // ============================================================
    // 6️⃣ AI-BASED COMPENSATION HELPERS (SAGA)
    // ============================================================
    public int deleteById(String table, String idColumn, Object idValue) {
        String whereClause = idColumn + " = " + (idValue instanceof String ? "'" + idValue + "'" : idValue);
        String query = String.format("DELETE FROM %s WHERE %s", table, whereClause);
        log.debug("AI-compensating delete: {}", query);
        List<Map<String, Object>> result = executeQuery(query);
        Object rows = result.get(0).get("rows_affected");
        return rows == null ? 0 : ((Number) rows).intValue();
    }

    public int updateById(String table, String idColumn, Object idValue, Map<String, Object> oldValues) {
        if (oldValues == null || oldValues.isEmpty())
            throw new IllegalArgumentException("oldValues cannot be empty");

        String setSql = oldValues.entrySet().stream()
                .map(e -> e.getValue() instanceof String
                        ? e.getKey() + " = '" + e.getValue() + "'"
                        : e.getKey() + " = " + e.getValue())
                .collect(Collectors.joining(", "));

        String whereClause = idColumn + " = " + (idValue instanceof String ? "'" + idValue + "'" : idValue);
        String query = String.format("UPDATE %s SET %s WHERE %s", table, setSql, whereClause);
        log.debug("AI-compensating update: {}", query);
        List<Map<String, Object>> result = executeQuery(query);
        Object rows = result.get(0).get("rows_affected");
        return rows == null ? 0 : ((Number) rows).intValue();
    }
}
