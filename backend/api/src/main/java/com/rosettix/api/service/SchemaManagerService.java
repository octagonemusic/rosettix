package com.rosettix.api.service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class SchemaManagerService {

    private final JdbcTemplate jdbcTemplate;

    public String getDatabaseSchema() {
        // SQL query to get table names, column names, and data types from the database's metadata
        String sql =
            "SELECT table_name, column_name, data_type " +
            "FROM information_schema.columns " +
            "WHERE table_schema = 'public' " + // 'public' is the default schema in PostgreSQL
            "ORDER BY table_name, ordinal_position;";

        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

        // Group columns by table name
        Map<String, List<String>> tables = rows
            .stream()
            .collect(
                Collectors.groupingBy(
                    row -> (String) row.get("table_name"),
                    Collectors.mapping(
                        row ->
                            (String) row.get("column_name") +
                            " (" +
                            row.get("data_type") +
                            ")",
                        Collectors.toList()
                    )
                )
            );

        // Format the schema into a clean string for the LLM prompt
        StringBuilder schemaBuilder = new StringBuilder();
        tables.forEach((tableName, columns) -> {
            schemaBuilder
                .append("CREATE TABLE ")
                .append(tableName)
                .append(" (")
                .append(String.join(", ", columns))
                .append(");\n");
        });

        return schemaBuilder.toString();
    }
}
