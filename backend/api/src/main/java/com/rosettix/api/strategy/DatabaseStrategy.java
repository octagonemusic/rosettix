package com.rosettix.api.strategy;

import java.util.List;
import java.util.Map;

public interface DatabaseStrategy {
    String getSchemaRepresentation();
    String getQueryDialect();
    List<Map<String, Object>> executeQuery(String query);
}
