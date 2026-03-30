package com.rosettix.api.strategy;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.ListCollectionNamesIterable;
import com.rosettix.api.config.RosettixConfiguration;
import com.rosettix.api.service.SchemaCacheService;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongoStrategyTest {

    @SuppressWarnings("unchecked")
    @Test
    void reusesCachedSchemaWithinTtl() {
        MongoTemplate mongoTemplate = mock(MongoTemplate.class);
        MongoDatabase mongoDatabase = mock(MongoDatabase.class);
        ListCollectionNamesIterable collections = mock(ListCollectionNamesIterable.class);
        MongoCursor<String> cursor = mock(MongoCursor.class);
        MongoCollection<Document> mongoCollection = mock(MongoCollection.class);
        FindIterable<Document> findIterable = mock(FindIterable.class);

        when(mongoTemplate.getDb()).thenReturn(mongoDatabase);
        when(mongoDatabase.listCollectionNames()).thenReturn(collections);
        when(collections.iterator()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next()).thenReturn("users");
        when(mongoTemplate.getCollection("users")).thenReturn(mongoCollection);
        when(mongoCollection.find()).thenReturn(findIterable);
        when(findIterable.limit(3)).thenReturn(findIterable);
        when(findIterable.into(org.mockito.ArgumentMatchers.anyList())).thenAnswer(invocation -> {
            List<Document> target = invocation.getArgument(0);
            target.add(new Document("_id", 1).append("name", "Alice").append("email", "a@b.com"));
            return target;
        });

        RosettixConfiguration configuration = new RosettixConfiguration();
        configuration.getSchemaCache().setEnabled(true);
        configuration.getSchemaCache().setTtlMinutes(5);
        SchemaCacheService schemaCacheService = new SchemaCacheService(configuration);
        MongoStrategy strategy = new MongoStrategy(mongoTemplate, schemaCacheService);

        String first = strategy.getSchemaRepresentation();
        String second = strategy.getSchemaRepresentation();

        assertEquals("users(_id, name, email); ", first);
        assertEquals(first, second);
        verify(mongoDatabase, times(1)).listCollectionNames();
        verify(mongoCollection, times(1)).find();
    }
}
