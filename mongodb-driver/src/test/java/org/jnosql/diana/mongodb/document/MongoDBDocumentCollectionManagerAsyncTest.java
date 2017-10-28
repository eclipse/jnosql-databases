/*
 *  Copyright (c) 2017 Otávio Santana and others
 *   All rights reserved. This program and the accompanying materials
 *   are made available under the terms of the Eclipse Public License v1.0
 *   and Apache License v2.0 which accompanies this distribution.
 *   The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 *   and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 *   You may elect to redistribute this code under either of these licenses.
 *
 *   Contributors:
 *
 *   Otavio Santana
 */
package org.jnosql.diana.mongodb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.api.document.query.DocumentQueryBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.jnosql.diana.api.document.DocumentCondition.eq;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.jnosql.diana.mongodb.document.DocumentConfigurationUtils.getAsync;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class MongoDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";
    private static final long WAIT_TIME = 1_000L;

    private static DocumentCollectionManagerAsync entityManager;

    @BeforeClass
    public static void setUp() throws IOException, InterruptedException {
        MongoDbHelper.startMongoDb();
        entityManager = getAsync().getAsync("database");
        Thread.sleep(WAIT_TIME);
    }


    @Test
    public void shouldSaveAsync() throws InterruptedException {
        AtomicBoolean condition = new AtomicBoolean(false);
        DocumentEntity entity = getEntity();
        entityManager.insert(entity, c -> condition.set(true));
        Thread.sleep(WAIT_TIME);

    }

    @Test
    public void shouldSelect() throws InterruptedException {
        DocumentEntity entity = getEntity();
        for (int index = 0; index < 10; index++) {
            entityManager.insert(entity);
        }
        AtomicBoolean condition = new AtomicBoolean(false);
        Thread.sleep(WAIT_TIME);
        DocumentQuery query = select().from(entity.getName()).build();
        entityManager.select(query, c -> condition.set(true));
        Thread.sleep(WAIT_TIME);
        assertTrue(condition.get());
    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManager.update(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() throws InterruptedException {
        AtomicReference<DocumentEntity> entityAtomic = new AtomicReference<>();
        entityManager.insert(getEntity(), e -> entityAtomic.set(e));
        Thread.sleep(WAIT_TIME);
        DocumentEntity entity = entityAtomic.get();
        assertNotNull(entity);
        String collection = entity.getName();
        DocumentCondition documentCondition = eq(entity.find("name").get());
        DocumentDeleteQuery deleteQuery = delete().from(collection)
                .where(documentCondition)
                .build();
        AtomicBoolean condition = new AtomicBoolean(false);
        entityManager.delete(deleteQuery, c -> condition.set(true));
        Thread.sleep(WAIT_TIME);
        assertTrue(condition.get());

    }

    private DocumentEntity getEntity() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Poliana");
        map.put("city", "Salvador");
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }

    @AfterClass
    public static void end() {
        MongoDbHelper.stopMongoDb();
    }
}