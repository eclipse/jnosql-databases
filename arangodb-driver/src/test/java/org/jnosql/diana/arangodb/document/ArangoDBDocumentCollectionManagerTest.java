/*
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

package org.jnosql.diana.arangodb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.jnosql.diana.arangodb.document.DocumentConfigurationUtils.getConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class ArangoDBDocumentCollectionManagerTest {

    public static final String COLLECTION_NAME = "person";
    private DocumentCollectionManager entityManager;
    private Random random;
    private String KEY_NAME = "_key";

    @Before
    public void setUp() {
        random = new Random();
        entityManager = getConfiguration().get("database");
    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();

        DocumentEntity documentEntity = entityManager.save(entity);
        assertTrue(documentEntity.getDocuments().stream().map(Document::getName).anyMatch(s -> s.equals(KEY_NAME)));
    }

    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldRemoveEntity() {
        DocumentEntity documentEntity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("_key");
        query.and(DocumentCondition.eq(id.get()));
        entityManager.delete(DocumentDeleteQuery.of(COLLECTION_NAME, DocumentCondition.eq(id.get())));
        assertTrue(entityManager.find(query).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find(KEY_NAME);
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.find(query);
        assertFalse(entities.isEmpty());
        DocumentEntity documentEntity = entities.get(0);
        assertEquals(entity.find(KEY_NAME).get().getValue().get(String.class), documentEntity.find(KEY_NAME).get()
                .getValue().get(String.class));
        assertEquals(entity.find("name").get(), documentEntity.find("name").get());
        assertEquals(entity.find("city").get(), documentEntity.find("city").get());
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Collections.singletonMap("mobile","1231231")));
        DocumentEntity entitySaved = entityManager.save(entity);
        Document id = entitySaved.find(KEY_NAME).get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.find(query).get(0);
        Map<String, String> result = (Map<String, String>) entityFound.find("phones").get().getValue().get();
        String key = result.keySet().stream().findFirst().get();
        String value = result.get(key);
        assertEquals("mobile", key);
        assertEquals("1231231", value);
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