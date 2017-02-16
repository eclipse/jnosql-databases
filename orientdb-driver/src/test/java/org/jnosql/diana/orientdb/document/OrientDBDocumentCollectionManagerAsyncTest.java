/*
 * Copyright 2017 Eclipse Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.orientdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.jnosql.diana.orientdb.document.DocumentConfigurationUtils.getAsync;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class OrientDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private DocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    @Before
    public void setUp() {
        entityManagerAsync = getAsync().getAsync("database");
        entityManager = DocumentConfigurationUtils.get().get("database");
        DocumentEntity documentEntity = getEntity();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        try {
            entityManagerAsync.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        } catch (Exception e) {

        }
    }


    @Test
    public void shouldSaveAsync() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManagerAsync.save(entity);

        Thread.sleep(1_000L);
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.find(query);
        assertFalse(entities.isEmpty());

    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        entityManagerAsync.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        Thread.sleep(1_000L);
        assertTrue(entityManager.find(query).isEmpty());

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