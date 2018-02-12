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
package org.jnosql.diana.arangodb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.Documents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.awaitility.Awaitility.await;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.arangodb.document.DocumentConfigurationUtils.getConfiguration;


public class ArangoDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";
    private DocumentCollectionManagerAsync entityManagerAsync;
    private DocumentCollectionManager entityManager;
    private Random random;
    private String KEY_NAME = "_key";

    @BeforeEach
    public void setUp() {
        random = new Random();
        entityManagerAsync = getConfiguration().getAsync("database");
        entityManager = getConfiguration().get("database");
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

        entityManagerAsync.insert(entity, d -> {
            condition.set(true);
        });

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
    public void shouldSeletc() {
        DocumentEntity entity = getEntity();
        AtomicReference<DocumentEntity> reference = new AtomicReference<>();
        AtomicBoolean condition = new AtomicBoolean();

        entityManagerAsync.insert(entity);
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