package com.rosettix.api.service;

import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DbExecutionService {

    private final JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> executeSql(String sql) {
        return jdbcTemplate.queryForList(sql);
    }
}
