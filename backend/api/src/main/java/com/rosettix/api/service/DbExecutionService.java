package com.rosettix.api.service;

import java.util.List;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class DbExecutionService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<Map<String, Object>> executeSql(String sql) {
        return jdbcTemplate.queryForList(sql);
    }
}
