/*
 * Copyright 2017 Otavio Santana and others
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

package org.jnosql.diana.mongodb.document;

import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.hamcrest.Matchers.contains;
import static org.jnosql.diana.mongodb.document.DocumentConfigurationUtils.get;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class MongoDBDocumentCollectionManagerTest {

    public static final String COLLECTION_NAME = "person";
    private DocumentCollectionManager entityManager;

    @Before
    public void setUp() {
        entityManager = get().get("database");
    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        assertTrue(documentEntity.getDocuments().stream().map(Document::getName).anyMatch(s -> s.equals("_id")));
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
        Optional<Document> id = documentEntity.find("_id");
        query.and(DocumentCondition.eq(id.get()));
        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.find(query).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("_id");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.find(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.save(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.find(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        Map<String, String> result =  subDocument.get(new TypeReference<Map<String, String>>() {});
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


    public void init() {
        DocumentEntity oReilly = DocumentEntity.of("oReilly");
        oReilly.add(Document.of("_id", "oreilly"));
        oReilly.add(Document.of("name", "O'Reilly Media"));
        oReilly.add(Document.of("founded", "1980"));
        oReilly.add(Document.of("location", "CA"));
        oReilly.add(Document.of("books", Arrays.asList("123456789", "234567890")));

        DocumentEntity mongoBook = DocumentEntity.of("book");
        mongoBook.add(Document.of("_id", "123456789"));
        mongoBook.add(Document.of("title", "MongoDB: The Definitive Guide"));
        mongoBook.add(Document.of("author", Arrays.asList("ristina Chodorow", "Mike Dirolf")));
        mongoBook.add(Document.of("published_date", "2010-09-24"));
        mongoBook.add(Document.of("pages", 216));
        mongoBook.add(Document.of("language", "English"));
        mongoBook.add(Document.of("publisher_id", "oreilly"));

        DocumentEntity mongoDBDeveloper = DocumentEntity.of("book");
        mongoDBDeveloper.add(Document.of("_id", "234567890"));
        mongoDBDeveloper.add(Document.of("title", "50 Tips and Tricks for MongoDB Developer"));
        mongoDBDeveloper.add(Document.of("author", Arrays.asList("Kristina Chodorow")));
        mongoDBDeveloper.add(Document.of("published_date", "2011-05-06"));
        mongoDBDeveloper.add(Document.of("pages", 68));
        mongoDBDeveloper.add(Document.of("language", "English"));
        mongoDBDeveloper.add(Document.of("publisher_id", "oreilly"));
    }


}