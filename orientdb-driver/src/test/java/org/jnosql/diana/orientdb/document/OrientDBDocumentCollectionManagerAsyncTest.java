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
package org.jnosql.diana.orientdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.notNullValue;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.jnosql.diana.orientdb.document.DocumentConfigurationUtils.getAsync;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class OrientDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private OrientDBDocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    @BeforeEach
    public void setUp() {
        entityManagerAsync = getAsync().getAsync(Database.DATABASE);
        entityManager = DocumentConfigurationUtils.get().get(Database.DATABASE);
    }


    @Test
    public void shouldInsertAsync() throws InterruptedException {
        AtomicReference<DocumentEntity> entityAtomic = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), entityAtomic::set);
        await().until(entityAtomic::get, notNullValue(DocumentEntity.class));

        DocumentEntity entity = entityAtomic.get();
        Document id = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());

    }

    @Test
    public void ShouldThrowExceptionWhenInsertWithTTL() {
        assertThrows(UnsupportedOperationException.class, () -> entityManagerAsync.insert(getEntity(), Duration.ZERO));
    }

    @Test
    public void ShouldThrowExceptionWhenInsertWithTTLAndCallback() {
        assertThrows(UnsupportedOperationException.class, () -> entityManagerAsync.insert(getEntity(), Duration.ZERO, (d) -> {
        }));
    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);

        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldUpdateAsyncWithCallback() {
        final String NEW_FIELD_NAME = "newField2";
        final String NEW_FIELD_VALUE = "55";

        DocumentEntity entity = entityManager.insert(getEntity());
        Document newField = Documents.of(NEW_FIELD_NAME, NEW_FIELD_VALUE);
        entity.add(newField);

        AtomicBoolean condition = new AtomicBoolean(false);
        entityManagerAsync.update(entity, c -> condition.set(true));
        await().untilTrue(condition);

        Optional<Document> idDocument = entity.find(OrientDBConverter.RID_FIELD);
        DocumentQuery query = select().from(entity.getName())
                .where(idDocument.get().getName()).eq(idDocument.get().get())
                .build();
        Optional<DocumentEntity> entityUpdated = entityManager.singleResult(query);

        assertTrue(entityUpdated.isPresent());
        assertEquals(entityUpdated.get().find(NEW_FIELD_NAME).get(), newField);
    }

    @Test
    public void shouldRemoveEntityAsync() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        Document id = documentEntity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        AtomicBoolean condition = new AtomicBoolean(false);
        entityManagerAsync.delete(deleteQuery, c -> condition.set(true));
        await().untilTrue(condition);
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldRemoveEntityAsyncWithoutCallback() throws InterruptedException {
        DocumentEntity entity = entityManager.insert(getEntity());
        Document id = entity.find(OrientDBConverter.RID_FIELD).get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManagerAsync.delete(deleteQuery);
        Thread.sleep(1000L);
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldFindAsync() {
        DocumentEntity entity = entityManager.insert(getEntity());

        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        entityManagerAsync.select(query, reference::set);
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
        assertEquals(reference.get().get(0), entity);
    }

    @Test
    public void shouldFindAsyncWithNativeQuery() {
        DocumentEntity entity = entityManager.insert(getEntity());

        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        StringBuilder query = new StringBuilder().append("SELECT FROM ")
                .append(COLLECTION_NAME)
                .append(" WHERE name = ?");
        entityManagerAsync.sql(query.toString(), reference::set, "Poliana");
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
        assertEquals(reference.get().get(0), entity);
    }

    @Test
    public void shouldFindAsyncWithNativeQueryMapParam() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find(OrientDBConverter.RID_FIELD);
        Optional<Document> name = entity.find("name");

        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        Map<String, Object> params = new HashMap<>();
        params.put("id", id.get().get());
        params.put("name", name.get().get());

        entityManagerAsync.sql("select * from person where @rid = :id and name = :name",
                reference::set, params);
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
        assertEquals(reference.get().get(0), entity);
    }


    @Test
    public void shouldCount()  {
        AtomicReference<DocumentEntity> entityAtomic = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean(false);
        AtomicLong value = new AtomicLong(0L);

        entityManagerAsync.insert(getEntity(), entityAtomic::set);
        await().until(entityAtomic::get, notNullValue(DocumentEntity.class));

        Consumer<Long> callback = l -> {
            value.set(l);
            condition.set(true);

        };
        entityManagerAsync.count(COLLECTION_NAME, callback);
        await().untilTrue(condition);
        assertTrue(value.get() > 0);
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

    @AfterEach
    void removePersons() {
        DocumentDeleteQuery query = delete().from(COLLECTION_NAME).build();
        entityManager.delete(query);
    }
}