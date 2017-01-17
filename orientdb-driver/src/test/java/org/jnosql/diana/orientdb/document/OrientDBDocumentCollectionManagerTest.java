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
package org.jnosql.diana.orientdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.jnosql.diana.orientdb.document.DocumentConfigurationUtils.getConfiguration;
import static org.junit.Assert.*;


public class OrientDBDocumentCollectionManagerTest {
    public static final String COLLECTION_NAME = "person";
    private DocumentCollectionManager entityManager;

    @Before
    public void setUp() {
        entityManager = getConfiguration().getDocumentEntityManager("database");
    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        assertNotNull(documentEntity);
        assertEquals(entity, documentEntity);

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
        Optional<Document> id = documentEntity.find("name");
        query.addCondition(DocumentCondition.eq(id.get()));
        entityManager.delete(query);
        assertTrue(entityManager.find(query).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("name");
        query.addCondition(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.find(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldSaveAsync() {
        DocumentEntity entity = getEntity();
        entityManager.saveAsync(entity);

    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManager.updateAsync(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() {
        DocumentEntity documentEntity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("_id");
        query.addCondition(DocumentCondition.eq(id.get()));
        entityManager.deleteAsync(query);

    }

    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.save(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.addCondition(DocumentCondition.eq(id));
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
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}