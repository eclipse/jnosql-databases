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
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.jnosql.diana.mongodb.document.DocumentConfigurationUtils.getAsync;
import static org.junit.Assert.assertTrue;


public class MongoDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private static DocumentCollectionManagerAsync entityManager;

    @BeforeClass
    public static void setUp() throws IOException {
        MongoDbHelper.startMongoDb();
        entityManager = getAsync().getAsync("database");
    }


    @Test
    public void shouldSaveAsync() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);

    }

    @Test
    public void shouldSelete() throws InterruptedException {
        DocumentEntity entity = getEntity();
        for (int index = 0; index < 10; index++) {
            entityManager.insert(entity);
        }
        AtomicBoolean condition = new AtomicBoolean(false);
        Thread.sleep(1000L);
        DocumentQuery query = select().from(entity.getName()).build();
        entityManager.select(query, c -> condition.set(true));
        Thread.sleep(1000L);
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
    public void shouldRemoveEntityAsync() {
        entityManager.insert(getEntity());


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