package com.rosettix.api.strategy;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component("postgres") // We give it a name to find it later
@RequiredArgsConstructor
public class PostgresStrategy implements DatabaseStrategy {

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
    public String getQueryDialect() {
        return "PostgreSQL";
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        return jdbcTemplate.queryForList(query);
    }
}
