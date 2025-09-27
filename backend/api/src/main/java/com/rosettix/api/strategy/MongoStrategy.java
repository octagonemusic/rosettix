package com.rosettix.api.strategy;

import com.mongodb.client.MongoIterable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

@Component("mongodb")
@RequiredArgsConstructor
@Slf4j
public class MongoStrategy implements QueryStrategy {

    private final MongoTemplate mongoTemplate;

    @Override
    public String getSchemaRepresentation() {
        try {
            StringBuilder schemaBuilder = new StringBuilder();
            MongoIterable<String> collectionNames = mongoTemplate
                .getDb()
                .listCollectionNames();

            for (String collectionName : collectionNames) {
                // Skip system collections
                if (collectionName.startsWith("system.")) {
                    continue;
                }

                // Sample a few documents to understand the structure
                List<Document> sampleDocuments = mongoTemplate
                    .getCollection(collectionName)
                    .find()
                    .limit(5)
                    .into(new ArrayList<>());

                if (!sampleDocuments.isEmpty()) {
                    Map<String, String> fieldExamples = new HashMap<>();

                    // Collect fields with examples from sample documents
                    for (Document doc : sampleDocuments) {
                        for (Map.Entry<String, Object> entry : doc.entrySet()) {
                            String fieldName = entry.getKey();
                            if (!fieldExamples.containsKey(fieldName)) {
                                fieldExamples.put(
                                    fieldName,
                                    getFieldExample(entry.getValue())
                                );
                            }
                        }
                    }

                    schemaBuilder.append(collectionName).append("{");

                    fieldExamples
                        .entrySet()
                        .forEach(entry ->
                            schemaBuilder
                                .append(entry.getKey())
                                .append(": ")
                                .append(entry.getValue())
                                .append(", ")
                        );

                    // Remove trailing comma and space
                    if (schemaBuilder.length() > 2) {
                        schemaBuilder.setLength(schemaBuilder.length() - 2);
                    }

                    schemaBuilder.append("}; ");

                    log.debug(
                        "Collection '{}' schema: {}",
                        collectionName,
                        fieldExamples
                    );
                }
            }

            return schemaBuilder.length() > 0
                ? schemaBuilder.toString()
                : "No collections found";
        } catch (Exception e) {
            log.error("Error retrieving MongoDB schema: {}", e.getMessage(), e);
            return "Error retrieving schema";
        }
    }

    @Override
    public String getQueryLanguage() {
        return "MongoDB";
    }

    @Override
    public String getStrategyName() {
        return "mongodb";
    }

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.trim().isEmpty()) {
            return false;
        }

        // Basic safety checks for MongoDB queries
        String lowerQuery = query.toLowerCase().trim();

        // Prevent dangerous operations
        String[] dangerousOperations = {
            "db.dropdatabase",
            "db.collection.drop",
            "db.dropuser",
            "db.createuser",
            "eval(",
            "$where",
            "mapreduce",
            "db.runcommand",
        };

        for (String dangerous : dangerousOperations) {
            if (lowerQuery.contains(dangerous.toLowerCase())) {
                return false;
            }
        }

        // Only allow read operations for now
        return (
            lowerQuery.startsWith("db.") &&
            (lowerQuery.contains(".find(") ||
                lowerQuery.contains(".aggregate(") ||
                lowerQuery.contains(".count(") ||
                lowerQuery.contains(".distinct("))
        );
    }

    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        log.info("Starting MongoDB query execution: {}", query);

        try {
            // Step 1: Parse the MongoDB query
            log.debug("Step 1: Parsing MongoDB query structure");
            QueryParseResult parseResult = parseMongoQuery(query);

            if (parseResult == null) {
                log.error(
                    "PARSE ERROR: Could not parse MongoDB query syntax: {}",
                    query
                );
                log.error(
                    "Expected format: db.collection.find({{filter}}) or db.collection.count({{filter}})"
                );
                throw new IllegalArgumentException(
                    "MongoDB Query Parse Error: Invalid query syntax '" +
                        query +
                        "'. " +
                        "Expected format: db.collection.find({filter}) or db.collection.count({filter})"
                );
            }

            log.info(
                "Parsed query - Collection: {}, Operation: {}, Filter: '{}'",
                parseResult.collectionName,
                parseResult.operation,
                parseResult.filter
            );

            // Step 2: Execute the operation
            log.debug("Step 2: Executing MongoDB operation");
            return executeMongoOperation(parseResult);
        } catch (IllegalArgumentException e) {
            // Query parsing errors - don't wrap these
            throw e;
        } catch (com.mongodb.MongoException e) {
            log.error(
                "MONGODB ERROR: Database operation failed for query '{}': {}",
                query,
                e.getMessage(),
                e
            );
            throw new RuntimeException(
                "MongoDB Database Error: " +
                    e.getMessage() +
                    " (Query: " +
                    query +
                    ")",
                e
            );
        } catch (org.bson.json.JsonParseException e) {
            log.error(
                "JSON PARSE ERROR: Invalid filter JSON in query '{}': {}",
                query,
                e.getMessage(),
                e
            );
            throw new RuntimeException(
                "MongoDB Filter Parse Error: Invalid JSON in filter. " +
                    e.getMessage() +
                    " (Query: " +
                    query +
                    ")",
                e
            );
        } catch (Exception e) {
            log.error(
                "UNEXPECTED ERROR: Unknown error executing MongoDB query '{}': {}",
                query,
                e.getMessage(),
                e
            );
            throw new RuntimeException(
                "MongoDB Execution Error: " +
                    e.getClass().getSimpleName() +
                    ": " +
                    e.getMessage() +
                    " (Query: " +
                    query +
                    ")",
                e
            );
        }
    }

    private QueryParseResult parseMongoQuery(String query) {
        log.debug("Parsing MongoDB query: {}", query);

        try {
            String trimmed = query.trim();
            log.debug("Trimmed query: {}", trimmed);

            // Check if query starts with db.
            if (!trimmed.startsWith("db.")) {
                log.error(
                    "QUERY FORMAT ERROR: Query must start with 'db.', got: {}",
                    trimmed
                );
                return null;
            }

            // Extract collection name and operation
            String withoutDb = trimmed.substring(3); // Remove "db."
            log.debug("Without 'db.' prefix: {}", withoutDb);

            int dotIndex = withoutDb.indexOf('.');
            if (dotIndex == -1) {
                log.error(
                    "QUERY FORMAT ERROR: Missing collection.operation format, got: {}",
                    withoutDb
                );
                return null;
            }

            String collectionName = withoutDb.substring(0, dotIndex);
            String operationPart = withoutDb.substring(dotIndex + 1);

            log.debug(
                "Collection name: '{}', Operation part: '{}'",
                collectionName,
                operationPart
            );

            QueryParseResult result = new QueryParseResult();
            result.collectionName = collectionName;

            // Parse find operation
            if (operationPart.startsWith("find(")) {
                result.operation = "find";
                String filterPart = operationPart.substring(5); // Remove "find("
                int lastParen = filterPart.lastIndexOf(')');
                if (lastParen > 0) {
                    result.filter = filterPart.substring(0, lastParen);
                } else {
                    log.error(
                        "QUERY FORMAT ERROR: Missing closing parenthesis in find operation: {}",
                        operationPart
                    );
                    return null;
                }
                log.debug(
                    "Parsed find operation - Filter: '{}'",
                    result.filter
                );

                // Parse count operation
            } else if (operationPart.startsWith("count(")) {
                result.operation = "count";
                String filterPart = operationPart.substring(6); // Remove "count("
                int lastParen = filterPart.lastIndexOf(')');
                if (lastParen > 0) {
                    result.filter = filterPart.substring(0, lastParen);
                } else {
                    log.error(
                        "QUERY FORMAT ERROR: Missing closing parenthesis in count operation: {}",
                        operationPart
                    );
                    return null;
                }
                log.debug(
                    "Parsed count operation - Filter: '{}'",
                    result.filter
                );
            } else {
                log.error(
                    "UNSUPPORTED OPERATION: Operation '{}' not supported. Use find() or count()",
                    operationPart
                );
                return null;
            }

            log.info(
                "Successfully parsed query - Collection: '{}', Operation: '{}', Filter: '{}'",
                result.collectionName,
                result.operation,
                result.filter
            );
            return result;
        } catch (StringIndexOutOfBoundsException e) {
            log.error(
                "QUERY PARSE ERROR: String index error parsing query '{}': {}",
                query,
                e.getMessage(),
                e
            );
            return null;
        } catch (Exception e) {
            log.error(
                "QUERY PARSE ERROR: Unexpected error parsing query '{}': {}",
                query,
                e.getMessage(),
                e
            );
            return null;
        }
    }

    private List<Map<String, Object>> executeMongoOperation(
        QueryParseResult parseResult
    ) {
        List<Map<String, Object>> results = new ArrayList<>();

        try {
            log.debug(
                "Executing {} operation on collection '{}'",
                parseResult.operation,
                parseResult.collectionName
            );

            if ("find".equals(parseResult.operation)) {
                // Parse filter JSON
                Document filter;
                if (parseResult.filter.isEmpty()) {
                    filter = new Document();
                    log.debug("Using empty filter (find all documents)");
                } else {
                    log.debug("Parsing filter JSON: {}", parseResult.filter);
                    try {
                        filter = Document.parse(parseResult.filter);
                        log.debug(
                            "Successfully parsed filter: {}",
                            filter.toJson()
                        );
                    } catch (org.bson.json.JsonParseException e) {
                        log.error(
                            "FILTER PARSE ERROR: Invalid JSON in filter '{}': {}",
                            parseResult.filter,
                            e.getMessage()
                        );
                        throw new org.bson.json.JsonParseException(
                            "Invalid filter JSON '" +
                                parseResult.filter +
                                "': " +
                                e.getMessage(),
                            e
                        );
                    }
                }

                // Check if collection exists
                log.debug(
                    "Checking if collection '{}' exists",
                    parseResult.collectionName
                );
                boolean collectionExists = mongoTemplate
                    .getDb()
                    .listCollectionNames()
                    .into(new ArrayList<>())
                    .contains(parseResult.collectionName);

                if (!collectionExists) {
                    log.warn(
                        "COLLECTION NOT FOUND: Collection '{}' does not exist",
                        parseResult.collectionName
                    );
                    // Return empty results instead of throwing error
                    return results;
                }

                // Execute find operation
                log.debug(
                    "Executing find operation with filter: {}",
                    filter.toJson()
                );
                List<Document> documents = mongoTemplate
                    .getCollection(parseResult.collectionName)
                    .find(filter)
                    .limit(100) // Limit results for safety
                    .into(new ArrayList<>());

                log.info(
                    "Found {} documents in collection '{}'",
                    documents.size(),
                    parseResult.collectionName
                );

                // Convert documents to maps
                for (Document doc : documents) {
                    Map<String, Object> result = new HashMap<>();
                    for (Map.Entry<String, Object> entry : doc.entrySet()) {
                        result.put(entry.getKey(), entry.getValue());
                    }
                    results.add(result);
                }
            } else if ("count".equals(parseResult.operation)) {
                // Parse filter JSON
                Document filter;
                if (parseResult.filter.isEmpty()) {
                    filter = new Document();
                    log.debug(
                        "Using empty filter for count (count all documents)"
                    );
                } else {
                    log.debug(
                        "Parsing count filter JSON: {}",
                        parseResult.filter
                    );
                    try {
                        filter = Document.parse(parseResult.filter);
                        log.debug(
                            "Successfully parsed count filter: {}",
                            filter.toJson()
                        );
                    } catch (org.bson.json.JsonParseException e) {
                        log.error(
                            "COUNT FILTER PARSE ERROR: Invalid JSON in filter '{}': {}",
                            parseResult.filter,
                            e.getMessage()
                        );
                        throw new org.bson.json.JsonParseException(
                            "Invalid count filter JSON '" +
                                parseResult.filter +
                                "': " +
                                e.getMessage(),
                            e
                        );
                    }
                }

                // Check if collection exists
                log.debug(
                    "Checking if collection '{}' exists for count",
                    parseResult.collectionName
                );
                boolean collectionExists = mongoTemplate
                    .getDb()
                    .listCollectionNames()
                    .into(new ArrayList<>())
                    .contains(parseResult.collectionName);

                if (!collectionExists) {
                    log.warn(
                        "COLLECTION NOT FOUND: Collection '{}' does not exist for count",
                        parseResult.collectionName
                    );
                    Map<String, Object> result = new HashMap<>();
                    result.put("count", 0);
                    results.add(result);
                    return results;
                }

                // Execute count operation
                log.debug(
                    "Executing count operation with filter: {}",
                    filter.toJson()
                );
                long count = mongoTemplate
                    .getCollection(parseResult.collectionName)
                    .countDocuments(filter);

                log.info(
                    "Count result: {} documents in collection '{}'",
                    count,
                    parseResult.collectionName
                );

                Map<String, Object> result = new HashMap<>();
                result.put("count", count);
                results.add(result);
            } else {
                log.error(
                    "UNSUPPORTED OPERATION: Operation '{}' is not supported",
                    parseResult.operation
                );
                throw new IllegalArgumentException(
                    "Unsupported MongoDB operation: '" +
                        parseResult.operation +
                        "'. " +
                        "Supported operations: find, count"
                );
            }
        } catch (org.bson.json.JsonParseException e) {
            // Re-throw JSON parse errors as-is
            throw e;
        } catch (com.mongodb.MongoException e) {
            log.error(
                "MONGODB OPERATION ERROR: Database error during {} on '{}': {}",
                parseResult.operation,
                parseResult.collectionName,
                e.getMessage(),
                e
            );
            throw e;
        } catch (Exception e) {
            log.error(
                "MONGO EXECUTION ERROR: Unexpected error during {} on '{}': {}",
                parseResult.operation,
                parseResult.collectionName,
                e.getMessage(),
                e
            );
            throw new RuntimeException(
                "MongoDB execution error during " +
                    parseResult.operation +
                    " on '" +
                    parseResult.collectionName +
                    "': " +
                    e.getClass().getSimpleName() +
                    ": " +
                    e.getMessage(),
                e
            );
        }

        log.info(
            "Successfully completed {} operation, returning {} results",
            parseResult.operation,
            results.size()
        );
        return results;
    }

    @Override
    public String buildPrompt(String question, String schema) {
        return String.format(
            "Given the MongoDB collections with their fields and example values: \n%s\n---\n" +
                "Translate the question into a valid MongoDB query using the exact field names and value types shown above. " +
                "Use the format: db.collection.find({filter}) or db.collection.count({filter}). " +
                "For string matching, use exact values or regex patterns. " +
                "For example: db.users.find({city: \"London\"}) or db.users.find({name: /john/i}) for case-insensitive. " +
                "Do not add any explanation, comments, or markdown formatting. " +
                "Only return the MongoDB query.\nQuestion: \"%s\"",
            schema,
            question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        if (rawQuery == null) {
            return "";
        }

        // Remove common markdown code block delimiters
        String cleaned = rawQuery
            .replace("```sql", "")
            .replace("```mongodb", "")
            .replace("```javascript", "")
            .replace("```", "");

        // Trim whitespace
        cleaned = cleaned.trim();

        // If it doesn't start with db., add it (in case LLM forgot)
        if (!cleaned.startsWith("db.") && !cleaned.isEmpty()) {
            // Try to detect collection name patterns and fix common issues
            if (cleaned.contains(".find(") || cleaned.contains(".count(")) {
                // Already has collection operation, just prepend db.
                cleaned = "db." + cleaned;
            }
        }

        return cleaned;
    }

    /**
     * Get a representative example for a field value to help the LLM understand the data structure
     */
    private String getFieldExample(Object value) {
        if (value == null) {
            return "null";
        }

        String type = value.getClass().getSimpleName();

        if (value instanceof String) {
            String strValue = (String) value;
            // Truncate long strings but keep meaningful examples
            if (strValue.length() > 20) {
                return "String(e.g., \"" + strValue.substring(0, 17) + "...\")";
            }
            return "String(e.g., \"" + strValue + "\")";
        } else if (value instanceof Number) {
            return "Number(e.g., " + value + ")";
        } else if (value instanceof Boolean) {
            return "Boolean(e.g., " + value + ")";
        } else if (value instanceof java.util.Date) {
            return "Date(e.g., ISODate)";
        } else if (value instanceof org.bson.types.ObjectId) {
            return "ObjectId";
        } else if (value instanceof java.util.List) {
            return "Array";
        } else if (value instanceof Document) {
            return "Object";
        } else {
            return type;
        }
    }

    private static class QueryParseResult {

        String collectionName;
        String operation;
        String filter = "";
    }
}
