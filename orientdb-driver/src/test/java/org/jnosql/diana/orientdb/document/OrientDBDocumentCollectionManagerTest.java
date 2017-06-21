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

import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.jnosql.diana.orientdb.document.DocumentConfigurationUtils.get;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class OrientDBDocumentCollectionManagerTest {


    public static final String COLLECTION_NAME = "person";

    private OrientDBDocumentCollectionManager entityManager;

    @Before
    public void setUp() {
        entityManager = get().get("database");
        DocumentEntity documentEntity = getEntity();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        try {
            entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        } catch (Exception e) {

        }

    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertNotNull(documentEntity);
        Optional<Document> document = documentEntity.find(RID_FIELD);
        assertTrue(document.isPresent());

    }

    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldRemoveEntity() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("name").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, contains(Document.of("mobile", "1231231")));
    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Arrays.asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("name").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, containsInAnyOrder(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231")));
    }

    @Test
    public void shouldQueryAnd() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("age", 24));
        entityManager.insert(entity);

        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);

        query.and(DocumentCondition.eq(Document.of("name", "Poliana"))
                .and(DocumentCondition.gte(Document.of("age", 10))));

        assertFalse(entityManager.select(query).isEmpty());

        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldQueryOr() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("age", 24));
        entityManager.insert(entity);

        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);

        query.and(DocumentCondition.eq(Document.of("name", "Poliana"))
                .or(DocumentCondition.gte(Document.of("age", 100))));

        assertFalse(entityManager.select(query).isEmpty());

        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldLive() throws InterruptedException {
        List<DocumentEntity> entities = new ArrayList<>();
        Consumer<DocumentEntity> callback = entities::add;

        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("name");
        query.and(DocumentCondition.eq(id.get()));
        entityManager.live(query, callback);
        entityManager.insert(getEntity());
        Thread.sleep(3_000L);
        assertFalse(entities.isEmpty());
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