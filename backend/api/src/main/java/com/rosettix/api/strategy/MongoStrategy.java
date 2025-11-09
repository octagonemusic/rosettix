package com.rosettix.api.strategy;

import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Pattern;

import static com.mongodb.client.model.Filters.eq;

@Component("mongodb")
@RequiredArgsConstructor
@Slf4j
public class MongoStrategy implements QueryStrategy {

    private final MongoTemplate mongoTemplate;

    // ============================================================
    // 1️⃣ SCHEMA INTROSPECTION
    // ============================================================
    @Override
    public String getSchemaRepresentation() {
        try {
            StringBuilder schemaBuilder = new StringBuilder();
            MongoIterable<String> collections = mongoTemplate.getDb().listCollectionNames();

            for (String collectionName : collections) {
                if (collectionName.startsWith("system.")) continue;
                List<Document> samples = mongoTemplate.getCollection(collectionName)
                        .find().limit(3).into(new ArrayList<>());
                if (samples.isEmpty()) continue;

                Set<String> fields = new LinkedHashSet<>();
                for (Document doc : samples) fields.addAll(doc.keySet());

                schemaBuilder.append(collectionName)
                        .append("(")
                        .append(String.join(", ", fields))
                        .append("); ");
            }
            return schemaBuilder.length() == 0 ? "No collections found." : schemaBuilder.toString();
        } catch (Exception e) {
            log.error("Error reading Mongo schema: {}", e.getMessage(), e);
            return "Error retrieving MongoDB schema.";
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

    // ============================================================
    // 2️⃣ AI PROMPT GENERATION
    // ============================================================
    @Override
    public String buildPrompt(String question, String schema) {
        return String.format(
                "Given the MongoDB collections and their example fields: \n%s\n---\n" +
                        "Translate the question into a valid MongoDB query using this syntax:\n" +
                        "db.collection.find({filter}) or db.collection.count({filter}) " +
                        "or db.collection.insertOne({...}) or db.collection.updateOne({filter}, {update}) " +
                        "or db.collection.deleteOne({filter}).\n\n" +
                        "Important:\n" +
                        "• Use **valid JSON** format for filters and updates.\n" +
                        "• For string fields, you may use BSON-style regex: { field: { \"$regex\": \"pattern\", \"$options\": \"i\" } }.\n" +
                        "• For numeric fields (like IDs, ages, counts), use direct equality (e.g., { field: 9 }).\n" +
                        "• Do NOT use JavaScript-style regex like /pattern/i.\n" +
                        "• Always use field names and data types exactly as shown in the schema.\n" +
                        "• Avoid unsafe commands like eval(), $where, mapReduce, or db.runCommand.\n\n" +
                        "Return only the query (no markdown, no explanation).\nQuestion: \"%s\"",
                schema, question
        );
    }

    @Override
    public String cleanQuery(String rawQuery) {
        if (rawQuery == null) return "";
        return rawQuery
                .replace("```javascript", "")
                .replace("```mongodb", "")
                .replace("```", "")
                .trim()
                .replaceAll(";$", "");
    }

    // ============================================================
    // 3️⃣ SAFETY FILTER
    // ============================================================
    private static final Pattern UNSAFE = Pattern.compile(
            "(dropdatabase|drop|eval|mapreduce|db\\.runcommand|\\$where)",
            Pattern.CASE_INSENSITIVE
    );

    @Override
    public boolean isQuerySafe(String query) {
        if (query == null || query.isBlank()) return false;
        String lower = query.toLowerCase(Locale.ROOT);
        return lower.startsWith("db.")
                && !UNSAFE.matcher(lower).find()
                && (lower.contains(".find(")
                || lower.contains(".count(")
                || lower.contains(".insertone(")
                || lower.contains(".updateone(")
                || lower.contains(".deleteone("));
    }

    // ============================================================
    // 4️⃣ EXECUTION HANDLER
    // ============================================================
    @Override
    public List<Map<String, Object>> executeQuery(String query) {
        if (query == null || query.isBlank())
            throw new IllegalArgumentException("Query cannot be null or empty");

        if (!isQuerySafe(query))
            throw new SecurityException("Blocked unsafe MongoDB operation: " + query);

        log.info("Executing MongoDB query: {}", query);
        try {
            String lower = query.toLowerCase(Locale.ROOT);

            if (lower.contains(".find(")) return executeFind(query);
            else if (lower.contains(".count(")) return executeCount(query);
            else if (lower.contains(".insertone(")) return executeInsert(query);
            else if (lower.contains(".updateone(")) return executeUpdate(query);
            else if (lower.contains(".deleteone(")) return executeDelete(query);
            else
                throw new IllegalArgumentException(
                        "Only find(), count(), insertOne(), updateOne(), and deleteOne() are supported."
                );

        } catch (Exception e) {
            log.error("MongoDB execution error: {}", e.getMessage(), e);
            throw new RuntimeException("MongoDB execution error: " + e.getMessage(), e);
        }
    }

    // ============================================================
    // 5️⃣ EXECUTION HELPERS
    // ============================================================
    private String extractCollectionName(String query) {
        int start = query.indexOf("db.") + 3;
        int dot = query.indexOf('.', start);
        return query.substring(start, dot).trim();
    }

    private String extractContent(String query, String operation) {
        int start = query.toLowerCase(Locale.ROOT)
                .indexOf(operation.toLowerCase(Locale.ROOT) + "(") + operation.length() + 1;
        int end = query.lastIndexOf(")");
        return (end > start) ? query.substring(start, end).trim() : "{}";
    }

    private String normalizeRegex(String json) {
        if (json == null || json.isBlank()) return json;
        return json.replaceAll("/(.*?)/i", "{\"$regex\": \"$1\", \"$options\": \"i\"}");
    }

    private List<Map<String, Object>> executeFind(String query) {
        String collection = extractCollectionName(query);
        String filterJson = normalizeRegex(extractContent(query, "find"));
        Document filter = filterJson.isBlank() ? new Document() : Document.parse(filterJson);
        List<Document> docs = mongoTemplate.getCollection(collection)
                .find(filter).limit(100).into(new ArrayList<>());
        return new ArrayList<>(docs);
    }

    private List<Map<String, Object>> executeCount(String query) {
        String collection = extractCollectionName(query);
        String filterJson = normalizeRegex(extractContent(query, "count"));
        Document filter = filterJson.isBlank() ? new Document() : Document.parse(filterJson);
        long count = mongoTemplate.getCollection(collection).countDocuments(filter);
        return List.of(Map.of("count", count));
    }

    private List<Map<String, Object>> executeInsert(String query) {
        String collection = extractCollectionName(query);
        String docJson = extractContent(query, "insertone");
        Document doc = Document.parse(docJson);
        if (!doc.containsKey("_id")) doc.put("_id", new ObjectId());
        mongoTemplate.getDb().getCollection(collection).insertOne(doc);
        log.info("Inserted into {} with _id={}", collection, doc.getObjectId("_id"));
        return List.of(Map.of("inserted_id", doc.getObjectId("_id").toHexString()));
    }

    private List<Map<String, Object>> executeUpdate(String query) {
        String collection = extractCollectionName(query);
        String content = normalizeRegex(extractContent(query, "updateone"));
        String[] parts = content.split("\\},\\s*\\{", 2);
        if (parts.length != 2)
            throw new IllegalArgumentException("Invalid updateOne syntax. Expected two JSON arguments.");

        String filterJson = parts[0] + "}";
        String updateJson = "{" + parts[1];
        Document filter = Document.parse(filterJson);
        Document update = Document.parse(updateJson);

        UpdateResult result = mongoTemplate.getDb().getCollection(collection)
                .updateOne(filter, new Document("$set", update));
        log.info("Updated {} documents in {}", result.getModifiedCount(), collection);
        return List.of(Map.of("modified_count", result.getModifiedCount()));
    }

    private List<Map<String, Object>> executeDelete(String query) {
        String collection = extractCollectionName(query);
        String filterJson = normalizeRegex(extractContent(query, "deleteone"));
        Document filter = filterJson.isBlank() ? new Document() : Document.parse(filterJson);

        for (String key : new ArrayList<>(filter.keySet())) {
            Object value = filter.get(key);
            if (value instanceof Document valDoc && valDoc.containsKey("$regex")) {
                String regexValue = valDoc.getString("$regex");
                if (regexValue.matches("^\\^?\\d+\\$?$")) {
                    try {
                        int numeric = Integer.parseInt(regexValue.replaceAll("\\D", ""));
                        filter.put(key, numeric);
                        log.info("Auto-converted regex {} → numeric {}", regexValue, numeric);
                    } catch (NumberFormatException ignored) {}
                }
            }
        }

        log.info("Parsed filter for deletion: {}", filter.toJson());
        DeleteResult res = mongoTemplate.getDb().getCollection(collection).deleteOne(filter);
        log.info("Deleted {} documents from {}", res.getDeletedCount(), collection);
        return List.of(Map.of("deleted_count", res.getDeletedCount()));
    }

    // ============================================================
    // 6️⃣ AI-BASED SAFE WRITES (FOR SAGA)
    // ============================================================
    public ObjectId insert(String collectionName, Document doc) {
        if (doc == null) throw new IllegalArgumentException("Document cannot be null");
        if (!doc.containsKey("_id")) doc.put("_id", new ObjectId());
        String query = String.format("db.%s.insertOne(%s)", collectionName, doc.toJson());
        executeQuery(query);
        log.debug("AI-insert executed for {} with _id={}", collectionName, doc.getObjectId("_id"));
        return doc.getObjectId("_id");
    }

    public long updateOne(String collectionName, Document filter, Document update) {
        String query = String.format("db.%s.updateOne(%s, %s)", collectionName, filter.toJson(), update.toJson());
        List<Map<String, Object>> result = executeQuery(query);
        Object modified = result.get(0).get("modified_count");
        return modified == null ? 0 : ((Number) modified).longValue();
    }

    public long deleteOne(String collectionName, Document filter) {
        String query = String.format("db.%s.deleteOne(%s)", collectionName, filter.toJson());
        List<Map<String, Object>> result = executeQuery(query);
        Object deleted = result.get(0).get("deleted_count");
        return deleted == null ? 0 : ((Number) deleted).longValue();
    }

    // ============================================================
    // 7️⃣ AI-BASED COMPENSATION HELPERS (SAGA)
    // ============================================================
    public Document findById(String collectionName, ObjectId id) {
        String query = String.format("db.%s.find({ _id: ObjectId('%s') })", collectionName, id.toHexString());
        List<Map<String, Object>> result = executeQuery(query);
        return result.isEmpty() ? null : new Document(result.get(0));
    }

    public long restoreOneById(String collectionName, ObjectId id, Document oldState) {
        if (oldState == null) return 0;
        String filter = String.format("{ _id: ObjectId('%s') }", id.toHexString());
        String query = String.format("db.%s.updateOne(%s, %s)", collectionName, filter, oldState.toJson());
        List<Map<String, Object>> result = executeQuery(query);
        Object modified = result.get(0).get("modified_count");
        log.info("AI-restore executed for _id={} in {}", id, collectionName);
        return modified == null ? 0 : ((Number) modified).longValue();
    }
}
