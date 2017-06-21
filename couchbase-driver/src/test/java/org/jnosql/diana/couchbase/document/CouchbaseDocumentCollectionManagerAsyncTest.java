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

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class CouchbaseDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private DocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    @AfterClass
    public static void afterClass() throws InterruptedException {
        Thread.sleep(1_000L);
    }

    @Before
    public void setUp() {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManagerAsync = managerFactory.getAsync("default");
        entityManager = managerFactory.get("default");
        DocumentEntity documentEntity = getEntity();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        entityManagerAsync.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
    }


    @Test
    public void shouldSaveAsync() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManagerAsync.insert(entity);

        Thread.sleep(1_000L);
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());

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
    public void shouldRemoveEntityAsync() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        entityManagerAsync.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        Thread.sleep(1_000L);
        assertTrue(entityManager.select(query).isEmpty());

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