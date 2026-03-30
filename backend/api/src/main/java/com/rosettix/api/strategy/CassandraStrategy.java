package com.rosettix.api.strategy;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.exception.QueryException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component("cassandra")
@RequiredArgsConstructor
@Slf4j
public class CassandraStrategy implements QueryStrategy {

    private static final Pattern UNSAFE_PATTERN = Pattern.compile(
        "\\b(drop|truncate|alter|create|grant|revoke|use|describe)\\b",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern SYSTEM_KEYSPACE_PATTERN = Pattern.compile(
        "\\b(system|system_auth|system_schema|system_traces|system_views|system_virtual_schema)\\.",
        Pattern.CASE_INSENSITIVE
    );
    private static final Pattern DELETE_PATTERN = Pattern.compile(
        "(?is)^delete\\s+from\\s+([a-zA-Z0-9_.]+)\\s+where\\s+(.+)$"
    );
    private static final Pattern UPDATE_PATTERN = Pattern.compile(
        "(?is)^update\\s+([a-zA-Z0-9_.]+)\\s+set\\s+(.+?)\\s+where\\s+(.+)$"
    );
    private static final Pattern INSERT_PATTERN = Pattern.compile(
        "(?is)^insert\\s+into\\s+([a-zA-Z0-9_.]+)\\s*\\((.+)\\)\\s*values\\s*\\((.+)\\)$"
    );
    private static final Pattern SELECT_PATTERN = Pattern.compile(
        "(?is)^select\\s+.+?\\s+from\\s+([a-zA-Z0-9_.]+)(?:\\s+where\\s+(.+?))?(?:\\s+allow\\s+filtering)?$"
    );

    private final CqlSession cqlSession;
    private final RosettixConfiguration rosettixConfiguration;

    @Override
    public String getSchemaRepresentation() {
        try {
            Optional<KeyspaceMetadata> currentKeyspace = cqlSession.getKeyspace()
                .flatMap(keyspace -> cqlSession.getMetadata().getKeyspace(keyspace));

            if (currentKeyspace.isPresent()) {
                return formatKeyspaceSchema(currentKeyspace.get());
            }

            return cqlSession.getMetadata()
                .getKeyspaces()
                .values()
                .stream()
                .filter(keyspace -> !keyspace.getName().asInternal().startsWith("system"))
                .sorted(Comparator.comparing(keyspace -> keyspace.getName().asInternal()))
                .map(this::formatKeyspaceSchema)
                .filter(schema -> !schema.isBlank())
                .collect(Collectors.joining(" "));
        } catch (Exception e) {
            log.error("Error retrieving Cassandra schema: {}", e.getMessage(), e);
            return "Error retrieving Cassandra schema.";
        }
    }

    @Override
    public String getQueryLanguage() {
        return "Cassandra CQL";
    }

    @Override
    public String getStrategyName() {
        return "cassandra";
    }

    @Override
    public String buildPrompt(String question, String schema) {
        String activeKeyspace = getActiveKeyspaceName();
        return String.format(
            "Given the Cassandra keyspace schema:%n%s%n---%n" +
                "Translate the question into a single valid Cassandra CQL query.%n" +
                "Rules:%n" +
                "- Use only SELECT, INSERT, UPDATE, or DELETE.%n" +
                "- Use only the application keyspace '%s'. Never query system keyspaces such as system, system_auth, or system_schema.%n" +
                "- Do not generate DROP, TRUNCATE, ALTER, CREATE, BATCH, or schema-changing commands.%n" +
                "- Prefer queries that use the known partition key columns when filtering.%n" +
                "- For DELETE or UPDATE requests, do not invent UUIDs or primary-key values that were not provided by the user.%n" +
                "- Do not append ALLOW FILTERING to DELETE or UPDATE statements.%n" +
                "- Do not invent tables or columns that are not present in the schema.%n" +
                "- Return only the CQL query with no markdown or explanation.%n" +
                "Question: \"%s\"",
            schema,
            activeKeyspace,
            question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        if (rawQuery == null) {
            return "";
        }

        String cleaned = rawQuery
            .replace("```sql", "")
            .replace("```cql", "")
            .replace("```", "")
            .trim();

        return cleaned.replaceAll(";+\\s*$", "");
    }

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.isBlank()) {
            return false;
        }

        String cleaned = cleanQuery(query);
        String lower = cleaned.toLowerCase(Locale.ROOT);

        return (lower.startsWith("select")
            || lower.startsWith("insert")
            || lower.startsWith("update")
            || lower.startsWith("delete"))
            && !UNSAFE_PATTERN.matcher(lower).find()
            && !SYSTEM_KEYSPACE_PATTERN.matcher(lower).find()
            && !lower.contains("--")
            && !lower.contains("/*")
            && !cleaned.contains(";");
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }

        String cleanedQuery = cleanQuery(query);
        if (!isQuerySafe(cleanedQuery)) {
            throw new SecurityException("Blocked potentially unsafe Cassandra query.");
        }

        try {
            String lower = cleanedQuery.toLowerCase(Locale.ROOT);

            if (lower.startsWith("select")) {
                return mapRows(cqlSession.execute(cleanedQuery));
            }

            if (lower.startsWith("delete")) {
                return executeDelete(cleanedQuery);
            }

            if (lower.startsWith("update")) {
                return executeUpdate(cleanedQuery);
            }

            if (lower.startsWith("insert")) {
                return executeInsert(cleanedQuery);
            }

            throw new IllegalArgumentException("Unsupported Cassandra command: " + cleanedQuery);
        } catch (Exception e) {
            log.error("Cassandra execution error: {}", e.getMessage(), e);
            throw new RuntimeException("Cassandra execution error: " + e.getMessage(), e);
        }
    }

    private String formatKeyspaceSchema(KeyspaceMetadata keyspace) {
        String tables = keyspace.getTables()
            .entrySet()
            .stream()
            .sorted(Comparator.comparing(entry -> entry.getKey().asInternal()))
            .map(entry -> formatTableSchema(entry.getKey().asInternal(), entry.getValue()))
            .collect(Collectors.joining(" "));

        if (tables.isBlank()) {
            return "";
        }

        return keyspace.getName().asInternal() + ": " + tables;
    }

    private String formatTableSchema(String tableName, TableMetadata table) {
        List<String> columns = table.getColumns()
            .entrySet()
            .stream()
            .sorted(Comparator.comparing(entry -> entry.getKey().asInternal()))
            .map(entry -> formatColumn(entry.getKey().asInternal(), entry.getValue()))
            .toList();

        String partitionKeys = table.getPartitionKey().stream()
            .map(column -> column.getName().asInternal())
            .collect(Collectors.joining(", "));

        List<String> clusteringKeyNames = new ArrayList<>();
        for (ColumnMetadata clusteringColumn : table.getClusteringColumns().keySet()) {
            clusteringKeyNames.add(clusteringColumn.getName().asInternal());
        }
        String clusteringKeys = String.join(", ", clusteringKeyNames);

        StringBuilder builder = new StringBuilder();
        builder.append(tableName)
            .append("(")
            .append(String.join(", ", columns))
            .append(")");

        if (!partitionKeys.isBlank()) {
            builder.append(" partition_keys[").append(partitionKeys).append("]");
        }

        if (!clusteringKeys.isBlank()) {
            builder.append(" clustering_keys[").append(clusteringKeys).append("]");
        }

        builder.append(";");
        return builder.toString();
    }

    private String formatColumn(String columnName, ColumnMetadata metadata) {
        return columnName + ":" + metadata.getType().asCql(false, true);
    }

    private String getActiveKeyspaceName() {
        return cqlSession.getKeyspace()
            .map(keyspace -> keyspace.asInternal())
            .orElse("unknown");
    }

    public String adaptWriteQuery(String question, String query) {
        if (question == null || query == null) {
            return query;
        }

        String cleanedQuery = cleanQuery(query);
        String lowerQuestion = question.toLowerCase(Locale.ROOT);
        String lowerQuery = cleanedQuery.toLowerCase(Locale.ROOT);

        if (!lowerQuery.startsWith("select")) {
            return cleanedQuery;
        }

        if (containsDeleteIntent(lowerQuestion)) {
            java.util.regex.Matcher matcher = SELECT_PATTERN.matcher(cleanedQuery);
            if (matcher.matches() && matcher.group(2) != null) {
                String tableName = matcher.group(1).trim();
                String whereClause = stripAllowFiltering(matcher.group(2).trim());
                return String.format("DELETE FROM %s WHERE %s", tableName, whereClause);
            }
        }

        return cleanedQuery;
    }

    private List<Map<String, Object>> executeInsert(String query) {
        ResultSet resultSet = cqlSession.execute(query);
        Optional<String> insertedId = extractInsertedId(query);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("status", "ok");
        response.put("query_type", "insert");
        response.put("applied", resultSet.wasApplied());
        insertedId.ifPresent(id -> response.put("inserted_id", id));
        return List.of(response);
    }

    private List<Map<String, Object>> executeDelete(String query) {
        DeleteQueryParts parts = parseDeleteQuery(query);
        TableMetadata table = getTableMetadata(parts.keyspace(), parts.table());
        return executeDeleteByPrimaryKeyLookup(parts, table);
    }

    private List<Map<String, Object>> executeUpdate(String query) {
        UpdateQueryParts parts = parseUpdateQuery(query);
        TableMetadata table = getTableMetadata(parts.keyspace(), parts.table());
        return executeUpdateByPrimaryKeyLookup(parts, table);
    }

    private List<Map<String, Object>> executeDeleteByPrimaryKeyLookup(DeleteQueryParts parts, TableMetadata table) {
        List<Row> matchingRows = selectPrimaryKeyRows(parts.keyspace(), parts.table(), parts.whereClause(), table);
        ensureRowsMatched(matchingRows, "delete", parts.qualifiedTableName(), parts.whereClause());
        ensureMutationWithinLimit(matchingRows.size(), "delete", parts.qualifiedTableName(), parts.whereClause());
        int deletedCount = 0;

        for (Row row : matchingRows) {
            String deleteQuery = String.format(
                "DELETE FROM %s WHERE %s",
                parts.qualifiedTableName(),
                buildPrimaryKeyWhereClause(row, table)
            );
            cqlSession.execute(deleteQuery);
            deletedCount++;
        }

        return List.of(Map.of(
            "status", "ok",
            "query_type", "delete",
            "applied", true,
            "rows_affected", deletedCount
        ));
    }

    private List<Map<String, Object>> executeUpdateByPrimaryKeyLookup(UpdateQueryParts parts, TableMetadata table) {
        List<Row> matchingRows = selectPrimaryKeyRows(parts.keyspace(), parts.table(), parts.whereClause(), table);
        ensureRowsMatched(matchingRows, "update", parts.qualifiedTableName(), parts.whereClause());
        ensureMutationWithinLimit(matchingRows.size(), "update", parts.qualifiedTableName(), parts.whereClause());
        int updatedCount = 0;

        for (Row row : matchingRows) {
            String updateQuery = String.format(
                "UPDATE %s SET %s WHERE %s",
                parts.qualifiedTableName(),
                parts.setClause(),
                buildPrimaryKeyWhereClause(row, table)
            );
            cqlSession.execute(updateQuery);
            updatedCount++;
        }

        return List.of(Map.of(
            "status", "ok",
            "query_type", "update",
            "applied", true,
            "rows_affected", updatedCount
        ));
    }

    private List<Row> selectPrimaryKeyRows(String keyspace, String tableName, String whereClause, TableMetadata table) {
        String primaryKeySelect = getPrimaryKeyColumns(table).stream()
            .map(column -> column.getName().asInternal())
            .collect(Collectors.joining(", "));

        String filteredWhereClause = stripAllowFiltering(whereClause);
        String selectQuery = String.format(
            "SELECT %s FROM %s WHERE %s%s",
            primaryKeySelect,
            qualifyTableName(keyspace, tableName),
            filteredWhereClause,
            requiresPrimaryKeyRewrite(whereClause, table) ? " ALLOW FILTERING" : ""
        );

        return cqlSession.execute(selectQuery).all();
    }

    private boolean requiresPrimaryKeyRewrite(String whereClause, TableMetadata table) {
        String normalized = stripAllowFiltering(whereClause).toLowerCase(Locale.ROOT);

        if (whereClause.toLowerCase(Locale.ROOT).contains("allow filtering")) {
            return true;
        }

        if (normalized.contains(">") || normalized.contains("<")) {
            return true;
        }

        Set<String> equalityColumns = extractEqualityColumns(normalized);
        Set<String> primaryKeyColumns = getPrimaryKeyColumns(table).stream()
            .map(column -> column.getName().asInternal().toLowerCase(Locale.ROOT))
            .collect(Collectors.toCollection(HashSet::new));

        return !equalityColumns.containsAll(primaryKeyColumns);
    }

    private Set<String> extractEqualityColumns(String whereClause) {
        Set<String> columns = new HashSet<>();
        for (String clause : whereClause.split("(?i)\\s+and\\s+")) {
            String trimmed = clause.trim();
            int equalsIndex = trimmed.indexOf('=');
            if (equalsIndex > 0) {
                columns.add(trimmed.substring(0, equalsIndex).trim().replace("\"", ""));
            }
        }
        return columns;
    }

    private String buildPrimaryKeyWhereClause(Row row, TableMetadata table) {
        return getPrimaryKeyColumns(table).stream()
            .map(column -> column.getName().asInternal() + " = " + toCqlLiteral(row.getObject(column.getName())))
            .collect(Collectors.joining(" AND "));
    }

    private List<ColumnMetadata> getPrimaryKeyColumns(TableMetadata table) {
        List<ColumnMetadata> primaryKeyColumns = new ArrayList<>(table.getPartitionKey());
        primaryKeyColumns.addAll(table.getClusteringColumns().keySet());
        return primaryKeyColumns;
    }

    private String toCqlLiteral(Object value) {
        if (value == null) {
            return "null";
        }
        if (value instanceof String stringValue) {
            return "'" + stringValue.replace("'", "''") + "'";
        }
        return value.toString();
    }

    private Optional<String> extractInsertedId(String query) {
        java.util.regex.Matcher matcher = INSERT_PATTERN.matcher(query);
        if (!matcher.matches()) {
            return Optional.empty();
        }

        String[] columns = matcher.group(2).split("\\s*,\\s*");
        String[] values = matcher.group(3).split("\\s*,\\s*");
        for (int index = 0; index < Math.min(columns.length, values.length); index++) {
            if ("id".equalsIgnoreCase(columns[index].trim())) {
                return Optional.of(values[index].trim());
            }
        }
        return Optional.empty();
    }

    private DeleteQueryParts parseDeleteQuery(String query) {
        java.util.regex.Matcher matcher = DELETE_PATTERN.matcher(query);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Cassandra DELETE query: " + query);
        }

        String[] tableParts = splitQualifiedTableName(matcher.group(1).trim());
        return new DeleteQueryParts(tableParts[0], tableParts[1], matcher.group(2).trim());
    }

    private UpdateQueryParts parseUpdateQuery(String query) {
        java.util.regex.Matcher matcher = UPDATE_PATTERN.matcher(query);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid Cassandra UPDATE query: " + query);
        }

        String[] tableParts = splitQualifiedTableName(matcher.group(1).trim());
        return new UpdateQueryParts(tableParts[0], tableParts[1], matcher.group(2).trim(), matcher.group(3).trim());
    }

    private TableMetadata getTableMetadata(String keyspace, String tableName) {
        KeyspaceMetadata metadata = cqlSession.getMetadata()
            .getKeyspaces()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().asInternal().equalsIgnoreCase(keyspace))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown Cassandra keyspace: " + keyspace));

        return metadata.getTables()
            .entrySet()
            .stream()
            .filter(entry -> entry.getKey().asInternal().equalsIgnoreCase(tableName))
            .map(Map.Entry::getValue)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Unknown Cassandra table: " + keyspace + "." + tableName));
    }

    private String[] splitQualifiedTableName(String qualifiedTableName) {
        String[] parts = qualifiedTableName.split("\\.", 2);
        if (parts.length == 2) {
            return new String[]{parts[0], parts[1]};
        }
        return new String[]{getActiveKeyspaceName(), qualifiedTableName};
    }

    private String qualifyTableName(String keyspace, String tableName) {
        return keyspace + "." + tableName;
    }

    private String stripAllowFiltering(String whereClause) {
        return whereClause.replaceAll("(?i)\\s+allow\\s+filtering\\s*$", "").trim();
    }

    private boolean containsDeleteIntent(String question) {
        return question.contains(" delete ")
            || question.startsWith("delete ")
            || question.contains(" remove ")
            || question.startsWith("remove ")
            || question.contains(" drop ")
            || question.startsWith("drop ");
    }

    private void ensureRowsMatched(List<Row> matchingRows, String operation, String tableName, String whereClause) {
        if (!matchingRows.isEmpty()) {
            return;
        }

        throw new QueryException(
            String.format(
                "Cassandra %s matched no rows in %s for filter: %s",
                operation,
                tableName,
                stripAllowFiltering(whereClause)
            ),
            getStrategyName(),
            QueryException.ErrorType.EXECUTION_ERROR
        );
    }

    private List<Map<String, Object>> mapRows(ResultSet resultSet) {
        List<Map<String, Object>> rows = new ArrayList<>();
        ColumnDefinitions definitions = resultSet.getColumnDefinitions();
        int maxResultSize = rosettixConfiguration.getQuery().getMaxResultSize();

        for (Row row : resultSet) {
            if (rows.size() >= maxResultSize) {
                break;
            }
            Map<String, Object> mappedRow = new LinkedHashMap<>();
            definitions.forEach(definition ->
                mappedRow.put(definition.getName().asInternal(), row.getObject(definition.getName()))
            );
            rows.add(mappedRow);
        }

        return rows;
    }

    private void ensureMutationWithinLimit(int rowCount, String operation, String tableName, String whereClause) {
        int maxMutationRows = rosettixConfiguration.getCassandra().getMaxMutationRows();
        if (rowCount <= maxMutationRows) {
            return;
        }

        throw new QueryException(
            String.format(
                "Cassandra %s would affect %d rows in %s, exceeding the configured limit of %d for filter: %s",
                operation,
                rowCount,
                tableName,
                maxMutationRows,
                stripAllowFiltering(whereClause)
            ),
            getStrategyName(),
            QueryException.ErrorType.UNSUPPORTED_OPERATION
        );
    }

    private record DeleteQueryParts(String keyspace, String table, String whereClause) {
        private String qualifiedTableName() {
            return keyspace + "." + table;
        }
    }

    private record UpdateQueryParts(String keyspace, String table, String setClause, String whereClause) {
        private String qualifiedTableName() {
            return keyspace + "." + table;
        }
    }
}
