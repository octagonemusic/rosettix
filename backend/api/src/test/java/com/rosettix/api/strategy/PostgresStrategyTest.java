package com.rosettix.api.strategy;

import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.service.SchemaCacheService;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PostgresStrategyTest {

    @Test
    void reusesCachedSchemaWithinTtl() {
        JdbcTemplate jdbcTemplate = mock(JdbcTemplate.class);
        when(jdbcTemplate.queryForList(org.mockito.ArgumentMatchers.anyString())).thenReturn(List.of(
                Map.of("table_name", "users", "column_name", "id", "data_type", "integer"),
                Map.of("table_name", "users", "column_name", "email", "data_type", "varchar")
        ));

        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getSchemaCache().setEnabled(true);
        configuration.getSchemaCache().setTtlMinutes(5);
        SchemaCacheService schemaCacheService = new SchemaCacheService(configuration);
        PostgresStrategy strategy = new PostgresStrategy(jdbcTemplate, schemaCacheService);

        String first = strategy.getSchemaRepresentation();
        String second = strategy.getSchemaRepresentation();

        assertEquals("users(id, email); ", first);
        assertEquals(first, second);
        verify(jdbcTemplate, times(1)).queryForList(org.mockito.ArgumentMatchers.anyString());
    }
}
