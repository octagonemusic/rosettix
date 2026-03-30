package com.rosettix.api.strategy;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.rosettix.api.config.RosettixConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CassandraStrategyTest {

    private CqlSession cqlSession;
    private RosettixConfiguration configuration;
    private CassandraStrategy strategy;

    @BeforeEach
    void setUp() {
        cqlSession = mock(CqlSession.class);
        configuration = new RosettixConfiguration();
        configuration.getQuery().setMaxResultSize(1);
        configuration.getCassandra().setMaxMutationRows(2);
        strategy = new CassandraStrategy(cqlSession, configuration);
    }

    @Test
    void adaptWriteQueryTurnsSelectIntoDeleteForDeleteIntent() {
        String adapted = strategy.adaptWriteQuery(
            "remove users older than 30",
            "SELECT id FROM test.students WHERE age > 30 ALLOW FILTERING"
        );

        assertEquals("DELETE FROM test.students WHERE age > 30", adapted);
    }

    @Test
    void executeQuerySelectHonorsConfiguredMaxResultSize() {
        ResultSet resultSet = mock(ResultSet.class);
        ColumnDefinitions definitions = mock(ColumnDefinitions.class);
        ColumnDefinition idDefinition = mock(ColumnDefinition.class);
        CqlIdentifier idIdentifier = mock(CqlIdentifier.class);
        Row firstRow = mock(Row.class);
        Row secondRow = mock(Row.class);

        when(cqlSession.execute("SELECT * FROM test.students")).thenReturn(resultSet);
        when(resultSet.getColumnDefinitions()).thenReturn(definitions);
        when(definitions.iterator()).thenReturn(List.of(idDefinition).iterator());
        when(idDefinition.getName()).thenReturn(idIdentifier);
        when(idIdentifier.asInternal()).thenReturn("id");
        when(firstRow.getObject(any(CqlIdentifier.class))).thenReturn("one");
        when(secondRow.getObject(any(CqlIdentifier.class))).thenReturn("two");
        when(resultSet.iterator()).thenReturn(List.of(firstRow, secondRow).iterator());

        List<Map<String, Object>> rows = strategy.executeQuery("SELECT * FROM test.students");

        assertEquals(1, rows.size());
    }

    @Test
    void executeDeleteFailsWhenNoRowsMatch() {
        mockTableMetadata();

        ResultSet selectResult = mock(ResultSet.class);
        when(selectResult.all()).thenReturn(List.of());
        when(cqlSession.execute("SELECT id FROM test.students WHERE id = 123"))
            .thenReturn(selectResult);

        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> strategy.executeQuery("DELETE FROM test.students WHERE id = 123")
        );

        assertEquals(
            "Cassandra execution error: Cassandra delete matched no rows in test.students for filter: id = 123",
            thrown.getMessage()
        );
    }

    @Test
    void executeDeleteDeletesMatchedRowsByPrimaryKey() {
        mockTableMetadata();

        ResultSet selectResult = mock(ResultSet.class);
        ResultSet deleteResult = mock(ResultSet.class);
        Row row = mock(Row.class);
        CqlIdentifier idIdentifier = mock(CqlIdentifier.class);
        when(idIdentifier.asInternal()).thenReturn("id");

        when(selectResult.all()).thenReturn(List.of(row));
        when(cqlSession.execute("SELECT id FROM test.students WHERE id = 123"))
            .thenReturn(selectResult);
        when(cqlSession.execute("DELETE FROM test.students WHERE id = 123")).thenReturn(deleteResult);
        when(row.getObject(any(CqlIdentifier.class))).thenReturn(123);

        List<Map<String, Object>> result = strategy.executeQuery("DELETE FROM test.students WHERE id = 123");

        assertEquals(1, result.size());
        assertEquals(1, result.get(0).get("rows_affected"));
        verify(cqlSession, times(1)).execute("DELETE FROM test.students WHERE id = 123");
    }

    @Test
    void executeDeleteFailsWhenMutationExceedsLimit() {
        mockTableMetadata();
        configuration.getCassandra().setMaxMutationRows(1);

        ResultSet selectResult = mock(ResultSet.class);
        Row first = mock(Row.class);
        Row second = mock(Row.class);
        when(selectResult.all()).thenReturn(List.of(first, second));
        when(cqlSession.execute("SELECT id FROM test.students WHERE age > 20 ALLOW FILTERING"))
            .thenReturn(selectResult);

        RuntimeException thrown = assertThrows(
            RuntimeException.class,
            () -> strategy.executeQuery("DELETE FROM test.students WHERE age > 20")
        );

        assertEquals(
            "Cassandra execution error: Cassandra delete would affect 2 rows in test.students, exceeding the configured limit of 1 for filter: age > 20",
            thrown.getMessage()
        );
    }

    private void mockTableMetadata() {
        Metadata metadata = mock(Metadata.class);
        KeyspaceMetadata keyspaceMetadata = mock(KeyspaceMetadata.class);
        TableMetadata tableMetadata = mock(TableMetadata.class);
        ColumnMetadata idColumn = mock(ColumnMetadata.class);
        CqlIdentifier keyspaceId = mock(CqlIdentifier.class);
        CqlIdentifier tableId = mock(CqlIdentifier.class);
        CqlIdentifier idIdentifier = mock(CqlIdentifier.class);

        when(cqlSession.getMetadata()).thenReturn(metadata);
        when(cqlSession.getKeyspace()).thenReturn(Optional.of(keyspaceId));
        when(keyspaceId.asInternal()).thenReturn("test");
        when(metadata.getKeyspaces()).thenReturn(Map.of(keyspaceId, keyspaceMetadata));
        when(keyspaceMetadata.getTables()).thenReturn(Map.of(tableId, tableMetadata));
        when(tableId.asInternal()).thenReturn("students");
        when(idColumn.getName()).thenReturn(idIdentifier);
        when(idIdentifier.asInternal()).thenReturn("id");
        when(tableMetadata.getPartitionKey()).thenReturn(List.of(idColumn));
        when(tableMetadata.getClusteringColumns()).thenReturn(new LinkedHashMap<ColumnMetadata, ClusteringOrder>());
    }
}
