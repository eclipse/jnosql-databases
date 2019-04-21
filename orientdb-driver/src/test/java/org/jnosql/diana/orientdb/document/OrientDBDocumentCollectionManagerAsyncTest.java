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

import org.awaitility.Awaitility;
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


    @BeforeEach
    public void setUp() {
        entityManagerAsync = getAsync().getAsync(Database.DATABASE);
    }


    @Test
    public void shouldInsertAsync() {
        AtomicReference<DocumentEntity> entityAtomic = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), entityAtomic::set);
        await().until(entityAtomic::get, notNullValue(DocumentEntity.class));

        DocumentEntity entity = entityAtomic.get();
        Document id = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        AtomicReference<List<DocumentEntity>> entities = new AtomicReference<>();
        entityManagerAsync.select(query, entities::set);

        await().until(() -> entities.get() != null);
        assertFalse(entities.get().isEmpty());

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
       AtomicReference<DocumentEntity> entity = new AtomicReference<>();

        entityManagerAsync.insert(getEntity(), entity::set);

        Document newField = Documents.of("newField", "10");
        entity.get().add(newField);
        entityManagerAsync.update(entity.get());
    }

    @Test
    public void shouldUpdateAsyncWithCallback() {
        final String NEW_FIELD_NAME = "newField2";
        final String NEW_FIELD_VALUE = "55";

        AtomicReference<DocumentEntity> entity = new AtomicReference<>();

        entityManagerAsync.insert(getEntity(), entity::set);
        await().until(() -> entity.get() != null);

        Document newField = Documents.of(NEW_FIELD_NAME, NEW_FIELD_VALUE);
        entity.get().add(newField);

        AtomicBoolean condition = new AtomicBoolean(false);
        entityManagerAsync.update(entity.get(), c -> condition.set(true));
        await().untilTrue(condition);

        Optional<Document> idDocument = entity.get().find("name");
        DocumentQuery query = select().from(entity.get().getName())
                .where(idDocument.get().getName()).eq(idDocument.get().get())
                .build();

        AtomicReference<Optional<DocumentEntity>> entityUpdated = new AtomicReference<>();
        entityManagerAsync.singleResult(query, entityUpdated::set);

        await().until(() -> entityUpdated.get() != null);
        assertTrue(entityUpdated.get().isPresent());
        assertEquals(entityUpdated.get().get().find(NEW_FIELD_NAME).get(), newField);
    }

    @Test
    public void shouldRemoveEntityAsync() {
        AtomicBoolean condition = new AtomicBoolean(false);

        AtomicReference<DocumentEntity> documentEntity = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), documentEntity::set);

        Awaitility.await().until(() -> documentEntity.get() != null);

        Document id = documentEntity.get().find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManagerAsync.delete(deleteQuery, c -> condition.set(true));
        await().untilTrue(condition);
        AtomicReference<List<DocumentEntity>> entities = new AtomicReference<>();
        entityManagerAsync.select(query, entities::set);
        await().until(() -> entities.get() != null);
        assertTrue(entities.get().isEmpty());
    }

    @Test
    public void shouldRemoveEntityAsyncWithoutCallback() throws InterruptedException {
        AtomicReference<DocumentEntity> entity = new AtomicReference<>();

        entityManagerAsync.insert(getEntity(), entity::set);
        await().until(() -> entity.get() != null);
        Document id = entity.get().find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManagerAsync.delete(deleteQuery);
        Thread.sleep(1000L);
        AtomicReference<List<DocumentEntity>> entities = new AtomicReference<>();
        entityManagerAsync.select(query, entities::set);
        await().until(() -> entities.get() != null);
        assertTrue(entities.get().isEmpty());
    }

    @Test
    public void shouldFindAsync() {
        AtomicReference<DocumentEntity> entity = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), entity::set);
        await().until(() -> entity.get() != null);

        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        entityManagerAsync.select(query, reference::set);
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
    }

    @Test
    public void shouldFindAsyncWithNativeQuery() {
        AtomicReference<DocumentEntity> entity = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), entity::set);
        await().until(() -> entity.get() != null);
        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        StringBuilder query = new StringBuilder().append("SELECT FROM ")
                .append(COLLECTION_NAME)
                .append(" WHERE name = ?");
        entityManagerAsync.sql(query.toString(), reference::set, "Poliana");
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
    }

    @Test
    public void shouldFindAsyncWithNativeQueryMapParam() {
        AtomicReference<DocumentEntity> entity = new AtomicReference<>();
        entityManagerAsync.insert(getEntity(), entity::set);
        await().until(() -> entity.get() != null);
        Optional<Document> id = entity.get().find(OrientDBConverter.RID_FIELD);
        Optional<Document> name = entity.get().find("name");

        AtomicReference<List<DocumentEntity>> reference = new AtomicReference<>();
        Map<String, Object> params = new HashMap<>();
        params.put("id", id.get().get());
        params.put("name", name.get().get());

        entityManagerAsync.sql("select * from person where @rid = :id and name = :name",
                reference::set, params);
        await().until(reference::get, notNullValue(List.class));

        assertFalse(reference.get().isEmpty());
    }


    @Test
    public void shouldCount() {
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
        entityManagerAsync.insert(getEntity());
        DocumentDeleteQuery query = delete().from(COLLECTION_NAME).build();
        AtomicBoolean condition = new AtomicBoolean();
        entityManagerAsync.delete(query, v -> {condition.set(true);});
        await().untilTrue(condition);
    }
}