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
package org.jnosql.diana.couchbase.document;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.couchbase.configuration.CouchbaseDocumentTcConfiguration;
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.couchbase.client.java.query.dsl.Expression.x;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.notNullValue;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private CouchbaseDocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    @AfterAll
    public static void afterClass() throws InterruptedException {
        Thread.sleep(1_000L);
    }

    @BeforeEach
    public void setUp() {
        CouchbaseDocumentConfiguration configuration = CouchbaseDocumentTcConfiguration.getTcConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManagerAsync = managerFactory.getAsync(CouchbaseUtil.BUCKET_NAME);
        entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);
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
    public void shouldInserAsyncTTL() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManagerAsync.insert(entity, Duration.ofSeconds(1L));

        TimeUnit.SECONDS.sleep(2L);
        Document id = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertTrue(entities.isEmpty());

    }

    @Test
    public void shouldInserAsyncCalbackTTL() throws InterruptedException {
        DocumentEntity entity = getEntity();
        AtomicBoolean condition = new AtomicBoolean(false);
        entityManagerAsync.insert(entity, Duration.ofSeconds(1L), d -> {
            condition.set(true);
        });

        await().untilTrue(condition);
        TimeUnit.SECONDS.sleep(2L);
        Document id = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertTrue(entities.isEmpty());

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
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
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
        assertFalse(references.get().isEmpty());

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

    @Test
    public void shouldRunN1Ql() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        TimeUnit.SECONDS.sleep(2L);
        await().atLeast(org.awaitility.Duration.TEN_SECONDS);
        AtomicReference<List<DocumentEntity>> references = new AtomicReference<>();
        entityManagerAsync.n1qlQuery("select * from jnosql", references::set);
        await().until(references::get, notNullValue());
        assertFalse(references.get().isEmpty());
    }

    @Test
    public void shouldRunN1QlParameters() {
        AtomicReference<List<DocumentEntity>> references = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean(false);

        DocumentEntity entity = getEntity();
        entityManagerAsync.insert(entity, d -> {
            condition.set(true);
        });
        await().untilTrue(condition);

        JsonObject params = JsonObject.create().put("name", "Poliana");
        entityManagerAsync.n1qlQuery("select * from jnosql where name = $name", params, references::set);
        await().until(references::get, notNullValue());
        assertEquals(1, references.get().size());
    }

    @Test
    public void shouldRunN1QlStatement() {
        AtomicReference<List<DocumentEntity>> references = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean(false);

        DocumentEntity entity = getEntity();
        entityManager.insert(entity);

        Statement statement = Select.select("*").from("jnosql").where(x("name").eq("\"Poliana\""));
        entityManagerAsync.n1qlQuery(statement, d -> {
            references.set(d);
            condition.set(true);
        });
        await().atLeast(org.awaitility.Duration.ONE_SECOND);
        await().untilTrue(condition);
        assertFalse(references.get().isEmpty());
        assertEquals(1, references.get().size());
    }


    private DocumentEntity getEntity() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Poliana");
        map.put("city", "Salvador");
        map.put("_id", "id");
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}