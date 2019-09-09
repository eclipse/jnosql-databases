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
package org.eclipse.jnosql.diana.arangodb.document;

import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import org.eclipse.jnosql.diana.document.Documents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class ArangoDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";
    private ArangoDBDocumentCollectionManagerAsync entityManagerAsync;
    private DocumentCollectionManager entityManager;
    private Random random;
    private String KEY_NAME = "_key";

    @BeforeEach
    public void setUp() {
        random = new Random();
        entityManagerAsync = ArangoDBDocumentCollectionManagerFactorySupplier.INSTANCE.get().getAsync("database");
        entityManager = ArangoDBDocumentCollectionManagerFactorySupplier.INSTANCE.get().get("database");
    }

    @Test
    public void shouldInserAsync() {
        DocumentEntity entity = getEntity();
        entityManagerAsync.insert(entity);
    }

    @Test
    public void shouldInsertCallBack() {
        DocumentEntity entity = getEntity();
        AtomicBoolean condition = new AtomicBoolean();

        entityManagerAsync.insert(entity, d -> condition.set(true));

        await().untilTrue(condition);
    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());

        Document id = documentEntity.find(KEY_NAME).get();
        DocumentDeleteQuery query = delete().from(COLLECTION_NAME).where(id.getName())
                .eq(id.get()).build();
        entityManagerAsync.delete(query);
    }

    @Test
    public void shouldSelect() {
        DocumentEntity entity = getEntity();
        AtomicReference<DocumentEntity> reference = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean(false);


        entityManagerAsync.insert(entity, d -> {
            condition.set(true);
            reference.set(d);
        });

        await().untilTrue(condition);

        DocumentEntity entity1 = reference.get();
        Document key = entity1.find("_key").get();

        condition.set(false);
        AtomicReference<Stream<DocumentEntity>> references = new AtomicReference<>();
        entityManagerAsync.select(select().from(entity1.getName()).where("_key").eq(key.get()).build(), l -> {
            condition.set(true);
            references.set(l);
        });
        await().untilTrue(condition);

        List<DocumentEntity> entities = references.get().collect(Collectors.toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldRunAQL() {
        DocumentEntity entity = getEntity();
        AtomicReference<DocumentEntity> reference = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean(false);


        entityManagerAsync.insert(entity, d -> {
            condition.set(true);
            reference.set(d);
        });

        await().untilTrue(condition);

        DocumentEntity entity1 = reference.get();
        Document key = entity1.find("_key").get();

        condition.set(false);
        AtomicReference<Stream<DocumentEntity>> references = new AtomicReference<>();
        entityManagerAsync.aql("FOR p IN person FILTER  p._key == @key RETURN p", Collections.singletonMap("key", key.get()), l -> {
            condition.set(true);
            references.set(l);
        });

        await().untilTrue(condition);
        List<DocumentEntity> entities = references.get().collect(Collectors.toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldCount() {
        DocumentEntity entity = getEntity();
        AtomicBoolean condition = new AtomicBoolean();
        AtomicLong value = new AtomicLong(0L);
        entityManagerAsync.insert(entity, d -> condition.set(true));

        await().untilTrue(condition);
        condition.set(false);
        Consumer<Long> callback = l -> {
            condition.set(true);
            value.set(l);
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
        entity.add(Document.of(KEY_NAME, random.nextLong()));
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}