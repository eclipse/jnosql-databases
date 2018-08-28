/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.couchdb.document.configuration.CouchDBDocumentTcConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.notNullValue;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CouchDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private CouchDBDocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    private static CouchDBDocumentConfiguration configuration;

    @BeforeAll
    public static void setupContainer() {
        configuration = CouchDBDocumentTcConfiguration.getTcConfiguration();
    }

    @AfterAll
    public static void afterClass() throws InterruptedException {
        Thread.sleep(1_00L);
    }

    @BeforeEach
    public void setUp() {
        CouchDBDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManagerAsync = managerFactory.getAsync("people");
        entityManager = managerFactory.get("people");
        DocumentEntity documentEntity = getEntity();
        Document id = documentEntity.find("name").get();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManagerAsync.delete(deleteQuery);
    }


    @Test
    public void shouldInsertAsync() {
        DocumentEntity entity = getEntity();
        AtomicBoolean condition = new AtomicBoolean(false);
        entityManagerAsync.insert(entity, d -> {
            condition.set(true);
        });

        await().untilTrue(condition);

        Document id = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());

    }


    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldUpdateCallbackAsync() {
        DocumentEntity entity = getEntity();
        AtomicReference<DocumentEntity> reference = new AtomicReference<>();
        entityManagerAsync.insert(entity, reference::set);
        await().until(() -> reference.get(), notNullValue());

        entity = reference.get();
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        reference.set(null);
        entityManagerAsync.update(entity, reference::set);
        await().until(() -> reference.get(), notNullValue());


        Document id = reference.get().find("_id").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        Optional<DocumentEntity> result = entityManager.singleResult(query);
        assertTrue(result.isPresent());
        assertEquals(newField, result.flatMap(d -> d.find("newField"))
                .orElseThrow(NullPointerException::new));

    }

    @Test
    public void shouldRemoveEntityAsync() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        Document id = documentEntity.find("name").get();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManagerAsync.delete(deleteQuery);
    }

    @Test
    public void shouldRemoveEntityAsyncCallBack() {
        AtomicBoolean condition = new AtomicBoolean(false);
        AtomicReference<List<DocumentEntity>> references = new AtomicReference<>();
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        Document id = documentEntity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManagerAsync.delete(deleteQuery, d -> {
            condition.set(true);
        });

        await().untilTrue(condition);
        entityManagerAsync.select(query, references::set);
        await().until(() -> references.get(), notNullValue());
        assertTrue(references.get().isEmpty());

    }

    @Test
    public void shouldSelect() {
        DocumentEntity entity = getEntity();
        AtomicReference<DocumentEntity> reference = new AtomicReference<>();
        AtomicReference<List<DocumentEntity>> references = new AtomicReference<>();

        entityManagerAsync.insert(entity, reference::set);

        await().until(() -> reference.get(), notNullValue());
        Document id = reference.get().find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManagerAsync.select(query, references::set);
        await().until(() -> references.get(), notNullValue());
        List<DocumentEntity> entities = references.get();
        assertFalse(entities.isEmpty());

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

}