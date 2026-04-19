package com.rosettix.api.strategy;

import com.rosettix.api.service.RedisSchemaCacheStore;
import com.rosettix.api.service.SchemaCacheService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.DataType;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component("redis")
@RequiredArgsConstructor
@Slf4j
public class RedisStrategy implements QueryStrategy {

    private static final Pattern TOKEN_PATTERN = Pattern.compile("\"((?:\\\\.|[^\"\\\\])*)\"|(\\S+)");
    private static final Set<String> READ_COMMANDS = Set.of(
            "GET", "HGET", "HGETALL", "LRANGE", "SMEMBERS", "ZRANGE", "TYPE", "EXISTS"
    );
    private static final Set<String> WRITE_COMMANDS = Set.of(
            "SET", "DEL", "HSET", "LPUSH", "RPUSH", "SADD", "ZADD", "EXPIRE"
    );
    private static final int MAX_SCHEMA_KEYS = 20;
    private static final int SAMPLE_COLLECTION_SIZE = 5;

    private final StringRedisTemplate redisTemplate;
    private final SchemaCacheService schemaCacheService;

    @Override
    public String getSchemaRepresentation() {
        return schemaCacheService.getSchema(getStrategyName(), this::fetchSchemaRepresentation);
    }

    private String fetchSchemaRepresentation() {
        try {
            Set<String> keys = redisTemplate.keys("*");
            if (keys == null || keys.isEmpty()) {
                return "No Redis keys found.";
            }

            List<String> visibleKeys = keys.stream()
                    .filter(this::isUserAccessibleKey)
                    .sorted()
                    .limit(MAX_SCHEMA_KEYS)
                    .toList();

            if (visibleKeys.isEmpty()) {
                return "No user-accessible Redis keys found.";
            }

            StringBuilder schema = new StringBuilder();
            for (String key : visibleKeys) {
                DataType dataType = redisTemplate.type(key);
                schema.append(key)
                        .append("(")
                        .append(dataType == null ? "unknown" : dataType.code())
                        .append("): ")
                        .append(describeValueSample(key, dataType))
                        .append("; ");
            }
            return schema.toString();
        } catch (Exception e) {
            log.error("Error reading Redis schema: {}", e.getMessage(), e);
            return "Error retrieving Redis schema.";
        }
    }

    @Override
    public String getQueryLanguage() {
        return "Redis";
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        if (query == null || query.isBlank()) {
            throw new IllegalArgumentException("Query cannot be null or empty");
        }

        List<String> tokens = tokenize(query);
        if (tokens.isEmpty()) {
            throw new IllegalArgumentException("Invalid Redis command");
        }

        String command = tokens.get(0).toUpperCase(Locale.ROOT);
        if (!READ_COMMANDS.contains(command) && !WRITE_COMMANDS.contains(command)) {
            throw new IllegalArgumentException("Unsupported Redis command: " + command);
        }

        ensureNoReservedKeyAccess(tokens);

        return switch (command) {
            case "GET" -> executeGet(tokens);
            case "HGET" -> executeHGet(tokens);
            case "HGETALL" -> executeHGetAll(tokens);
            case "LRANGE" -> executeLRange(tokens);
            case "SMEMBERS" -> executeSMembers(tokens);
            case "ZRANGE" -> executeZRange(tokens);
            case "TYPE" -> executeType(tokens);
            case "EXISTS" -> executeExists(tokens);
            case "SET" -> executeSet(tokens);
            case "DEL" -> executeDel(tokens);
            case "HSET" -> executeHSet(tokens);
            case "LPUSH" -> executeLPush(tokens);
            case "RPUSH" -> executeRPush(tokens);
            case "SADD" -> executeSAdd(tokens);
            case "ZADD" -> executeZAdd(tokens);
            case "EXPIRE" -> executeExpire(tokens);
            default -> throw new IllegalArgumentException("Unsupported Redis command: " + command);
        };
    }

    @Override
    public String getStrategyName() {
        return "redis";
    }

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.isBlank()) {
            return false;
        }

        String trimmed = query.trim();
        if (trimmed.contains("\n") || trimmed.contains("\r") || trimmed.contains(";")) {
            return false;
        }

        try {
            List<String> tokens = tokenize(trimmed);
            if (tokens.isEmpty()) {
                return false;
            }

            String command = tokens.get(0).toUpperCase(Locale.ROOT);
            boolean knownCommand = READ_COMMANDS.contains(command) || WRITE_COMMANDS.contains(command);
            if (!knownCommand) {
                return false;
            }

            ensureNoReservedKeyAccess(tokens);
            return isValidArity(command, tokens);
        } catch (Exception e) {
            return false;
        }
    }

    @Override
    public boolean isReadOperation(String query) {
        List<String> tokens = tokenize(query);
        return !tokens.isEmpty() && READ_COMMANDS.contains(tokens.get(0).toUpperCase(Locale.ROOT));
    }

    @Override
    public boolean isWriteOperation(String query) {
        List<String> tokens = tokenize(query);
        return !tokens.isEmpty() && WRITE_COMMANDS.contains(tokens.get(0).toUpperCase(Locale.ROOT));
    }

    @Override
    public String buildPrompt(String question, String schema) {
        return String.format(
                "Given the Redis keyspace summary: \n%s\n---\n" +
                        "Translate the question into exactly one valid Redis command.\n" +
                        "Allowed read commands: GET key, HGET key field, HGETALL key, LRANGE key start stop, " +
                        "SMEMBERS key, ZRANGE key start stop [WITHSCORES], TYPE key, EXISTS key.\n" +
                        "Allowed write commands: SET key value, DEL key, HSET key field value, LPUSH key value, " +
                        "RPUSH key value, SADD key value, ZADD key score member, EXPIRE key seconds.\n" +
                        "Use double quotes around arguments when they contain spaces.\n" +
                        "Do not use Lua, pipelines, transactions, CONFIG, FLUSH, KEYS, SCAN, or internal rosettix:schema:* keys.\n" +
                        "Return only the Redis command with no markdown or explanation.\nQuestion: \"%s\"",
                schema,
                question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        return QueryStrategy.super.cleanQuery(rawQuery)
                .replace("```bash", "")
                .replace("```shell", "")
                .trim();
    }

    private String describeValueSample(String key, DataType dataType) {
        if (dataType == null) {
            return "sample unavailable";
        }

        return switch (dataType.code()) {
            case "string" -> "value=" + quoteSample(redisTemplate.opsForValue().get(key));
            case "hash" -> "fields=" + sampleCollection(redisTemplate.opsForHash().keys(key));
            case "list" -> "items=" + sampleCollection(redisTemplate.opsForList().range(key, 0, SAMPLE_COLLECTION_SIZE - 1));
            case "set" -> "members=" + sampleCollection(redisTemplate.opsForSet().members(key));
            case "zset" -> "members=" + sampleZSet(redisTemplate.opsForZSet().rangeWithScores(key, 0, SAMPLE_COLLECTION_SIZE - 1));
            default -> "type=" + dataType.code();
        };
    }

    private List<Map<String, Object>> executeGet(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        String value = redisTemplate.opsForValue().get(key);
        return List.of(result("command", "GET", "key", key, "value", value));
    }

    private List<Map<String, Object>> executeHGet(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        String field = tokens.get(2);
        Object value = redisTemplate.opsForHash().get(key, field);
        return List.of(result("command", "HGET", "key", key, "field", field, "value", value));
    }

    private List<Map<String, Object>> executeHGetAll(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        Map<Object, Object> entries = redisTemplate.opsForHash().entries(key);
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("command", "HGETALL");
        response.put("key", key);
        response.put("entries", entries == null ? Collections.emptyMap() : entries);
        return List.of(response);
    }

    private List<Map<String, Object>> executeLRange(List<String> tokens) {
        expectArity(tokens, 4);
        String key = tokens.get(1);
        long start = parseLong(tokens.get(2), "start");
        long stop = parseLong(tokens.get(3), "stop");
        List<String> values = redisTemplate.opsForList().range(key, start, stop);
        return List.of(result("command", "LRANGE", "key", key, "values", values == null ? List.of() : values));
    }

    private List<Map<String, Object>> executeSMembers(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        Set<String> members = redisTemplate.opsForSet().members(key);
        return List.of(result("command", "SMEMBERS", "key", key, "members", members == null ? Set.of() : members));
    }

    private List<Map<String, Object>> executeZRange(List<String> tokens) {
        if (tokens.size() != 4 && tokens.size() != 5) {
            throw new IllegalArgumentException("ZRANGE requires key, start, stop, and optional WITHSCORES");
        }

        String key = tokens.get(1);
        long start = parseLong(tokens.get(2), "start");
        long stop = parseLong(tokens.get(3), "stop");
        boolean withScores = tokens.size() == 5 && "WITHSCORES".equalsIgnoreCase(tokens.get(4));
        if (tokens.size() == 5 && !withScores) {
            throw new IllegalArgumentException("ZRANGE only supports optional WITHSCORES");
        }

        if (withScores) {
            Set<ZSetOperations.TypedTuple<String>> tuples = redisTemplate.opsForZSet().rangeWithScores(key, start, stop);
            List<Map<String, Object>> members = new ArrayList<>();
            if (tuples != null) {
                for (ZSetOperations.TypedTuple<String> tuple : tuples) {
                    members.add(result("member", tuple.getValue(), "score", tuple.getScore()));
                }
            }
            return List.of(result("command", "ZRANGE", "key", key, "members", members));
        }

        Set<String> values = redisTemplate.opsForZSet().range(key, start, stop);
        return List.of(result("command", "ZRANGE", "key", key, "members", values == null ? Set.of() : values));
    }

    private List<Map<String, Object>> executeType(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        DataType type = redisTemplate.type(key);
        return List.of(result("command", "TYPE", "key", key, "type", type == null ? "none" : type.code()));
    }

    private List<Map<String, Object>> executeExists(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        Boolean exists = redisTemplate.hasKey(key);
        return List.of(result("command", "EXISTS", "key", key, "exists", Boolean.TRUE.equals(exists)));
    }

    private List<Map<String, Object>> executeSet(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        String value = tokens.get(2);
        redisTemplate.opsForValue().set(key, value);
        return List.of(result("command", "SET", "key", key, "status", "OK"));
    }

    private List<Map<String, Object>> executeDel(List<String> tokens) {
        expectArity(tokens, 2);
        String key = tokens.get(1);
        Boolean deleted = redisTemplate.delete(key);
        return List.of(result("command", "DEL", "key", key, "deleted", Boolean.TRUE.equals(deleted) ? 1L : 0L));
    }

    private List<Map<String, Object>> executeHSet(List<String> tokens) {
        expectArity(tokens, 4);
        String key = tokens.get(1);
        String field = tokens.get(2);
        String value = tokens.get(3);
        redisTemplate.opsForHash().put(key, field, value);
        return List.of(result("command", "HSET", "key", key, "field", field, "status", "OK"));
    }

    private List<Map<String, Object>> executeLPush(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        String value = tokens.get(2);
        Long size = redisTemplate.opsForList().leftPush(key, value);
        return List.of(result("command", "LPUSH", "key", key, "size", size));
    }

    private List<Map<String, Object>> executeRPush(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        String value = tokens.get(2);
        Long size = redisTemplate.opsForList().rightPush(key, value);
        return List.of(result("command", "RPUSH", "key", key, "size", size));
    }

    private List<Map<String, Object>> executeSAdd(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        String value = tokens.get(2);
        Long added = redisTemplate.opsForSet().add(key, value);
        return List.of(result("command", "SADD", "key", key, "added", added));
    }

    private List<Map<String, Object>> executeZAdd(List<String> tokens) {
        expectArity(tokens, 4);
        String key = tokens.get(1);
        double score = parseDouble(tokens.get(2), "score");
        String member = tokens.get(3);
        Boolean added = redisTemplate.opsForZSet().add(key, member, score);
        return List.of(result("command", "ZADD", "key", key, "member", member, "added", Boolean.TRUE.equals(added)));
    }

    private List<Map<String, Object>> executeExpire(List<String> tokens) {
        expectArity(tokens, 3);
        String key = tokens.get(1);
        long seconds = parseLong(tokens.get(2), "seconds");
        Boolean updated = redisTemplate.expire(key, java.time.Duration.ofSeconds(seconds));
        return List.of(result("command", "EXPIRE", "key", key, "updated", Boolean.TRUE.equals(updated)));
    }

    private List<String> tokenize(String query) {
        List<String> tokens = new ArrayList<>();
        if (query == null) {
            return tokens;
        }

        Matcher matcher = TOKEN_PATTERN.matcher(query);
        int consumed = 0;
        while (matcher.find()) {
            String quoted = matcher.group(1);
            String raw = matcher.group(2);
            tokens.add(quoted != null ? quoted.replace("\\\"", "\"") : raw);
            consumed = matcher.end();
        }

        if (tokens.isEmpty() || consumed != query.trim().length()) {
            throw new IllegalArgumentException("Unable to parse Redis command");
        }

        return tokens;
    }

    private void expectArity(List<String> tokens, int expectedSize) {
        if (tokens.size() != expectedSize) {
            throw new IllegalArgumentException(
                    "Expected " + (expectedSize - 1) + " arguments for " + tokens.get(0) + " but received " + (tokens.size() - 1)
            );
        }
    }

    private boolean isValidArity(String command, List<String> tokens) {
        return switch (command) {
            case "GET", "HGETALL", "SMEMBERS", "TYPE", "EXISTS", "DEL" -> tokens.size() == 2;
            case "HGET", "SET", "LPUSH", "RPUSH", "SADD", "EXPIRE" -> tokens.size() == 3;
            case "LRANGE", "HSET", "ZADD" -> tokens.size() == 4;
            case "ZRANGE" -> tokens.size() == 4 || tokens.size() == 5;
            default -> false;
        };
    }

    private long parseLong(String value, String argumentName) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for " + argumentName + ": " + value, e);
        }
    }

    private double parseDouble(String value, String argumentName) {
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid numeric value for " + argumentName + ": " + value, e);
        }
    }

    private boolean isUserAccessibleKey(String key) {
        return key != null && !key.startsWith(RedisSchemaCacheStore.KEY_PREFIX);
    }

    private void ensureNoReservedKeyAccess(List<String> tokens) {
        if (tokens.size() < 2) {
            return;
        }

        String key = tokens.get(1);
        if (!isUserAccessibleKey(key)) {
            throw new SecurityException("Access to internal Rosettix Redis keys is not allowed: " + key);
        }
    }

    private String sampleCollection(Collection<?> values) {
        if (values == null || values.isEmpty()) {
            return "[]";
        }

        return values.stream()
                .limit(SAMPLE_COLLECTION_SIZE)
                .map(String::valueOf)
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private String sampleZSet(Set<ZSetOperations.TypedTuple<String>> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            return "[]";
        }

        return tuples.stream()
                .sorted(Comparator.comparing(tuple -> String.valueOf(tuple.getValue())))
                .limit(SAMPLE_COLLECTION_SIZE)
                .map(tuple -> tuple.getValue() + ":" + tuple.getScore())
                .collect(Collectors.joining(", ", "[", "]"));
    }

    private String quoteSample(String value) {
        return value == null ? "null" : "\"" + value + "\"";
    }

    private Map<String, Object> result(Object... entries) {
        if (entries.length % 2 != 0) {
            throw new IllegalArgumentException("Result entries must be key-value pairs");
        }

        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i < entries.length; i += 2) {
            result.put(String.valueOf(entries[i]), entries[i + 1]);
        }
        return result;
    }
}
