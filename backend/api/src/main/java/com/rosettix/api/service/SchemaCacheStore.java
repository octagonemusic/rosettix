package com.rosettix.api.service;

import java.time.Duration;

public interface SchemaCacheStore {

    String get(String key);

    void put(String key, String schema, Duration ttl);

    String getStoreType();
}
