/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.api.document.query.DocumentQueryBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.xml.parsers.DocumentBuilder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultCouchDBDocumentCollectionManagerTest {

    public static final String COLLECTION_NAME = "person";

    private CouchDBDocumentCollectionManager entityManager;

    {
        CouchDBDocumentConfiguration configuration = new CouchDBDocumentConfiguration();
        CouchDBDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManager = managerFactory.get("people");
    }

    @BeforeEach
    public void setUp() {
        DocumentDeleteQuery query = DocumentQueryBuilder.delete().from(COLLECTION_NAME).build();
        entityManager.delete(query);
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }

    @Test
    public void shouldInsertNotId() {
        DocumentEntity entity = getEntity();
        entity.remove("_id");
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertTrue(documentEntity.find("_id").isPresent());
    }

    @Test
    public void shouldUpdate() {
        DocumentEntity entity = getEntity();
        entity.remove("_id");
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldReturnErrorOnUpdate() {
        assertThrows(NullPointerException.class, () -> entityManager.update((DocumentEntity) null));
        assertThrows(CouchDBHttpClientException.class, () -> {
            DocumentEntity entity = getEntity();
            entity.remove("_id");
            entityManager.update(entity);

        });

        assertThrows(CouchDBHttpClientException.class, () -> {
            DocumentEntity entity = getEntity();
            entity.add("_id", "not_found");
            entityManager.update(entity);

        });
    }


    @Test
    public void shouldSelect() {
        DocumentEntity entity = getEntity();
        entity.remove("_id");
        entity = entityManager.insert(entity);
        Object id = entity.find("_id").map(Document::get).get();
        DocumentQuery query = select().from(COLLECTION_NAME).where("_id").eq(id).build();
        DocumentEntity documentFound = entityManager.singleResult(query).get();
        assertEquals(entity, documentFound);
    }

    @Test
    public void shouldSelectEmptyResult() {
        DocumentQuery query = select().from(COLLECTION_NAME).where("no_field").eq("not_found").build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertTrue(entities.isEmpty());
    }


    @Test
    public void shouldRemoveEntityByName() {
        DocumentEntity entity = getEntity();
        entity.remove("_id");
        entity = entityManager.insert(entity);

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        DocumentDeleteQuery deleteQuery = DocumentQueryBuilder.delete().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get()).build();
        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldCount() {
        DocumentEntity entity = getEntity();
        entity.remove("_id");
        entityManager.insert(entity);
        long count = entityManager.count();
        assertTrue(count > 0);
    }

    @Test
    public void shouldSelectWithCouchDBDocumentQuery() {

        for (int index = 0; index < 4; index++) {
            DocumentEntity entity = getEntity();
            entity.remove("_id");
            entity.add("index", index);
            entityManager.insert(entity);
        }
        CouchDBDocumentQuery query = CouchDBDocumentQuery.of(select().from(COLLECTION_NAME)
                .where("index").in(asList(0, 1, 2, 3, 4)).limit(2).build());

        assertFalse(query.getBookmark().isPresent());
        List<DocumentEntity> entities = entityManager.select(query);
        assertEquals(2, entities.size());
        assertTrue(query.getBookmark().isPresent());
        String bookmark = query.getBookmark().get();

        entities = entityManager.select(query);
        assertEquals(2, entities.size());
        assertTrue(query.getBookmark().isPresent());
        assertNotEquals(bookmark, query.getBookmark().get());

        entities = entityManager.select(query);
        assertTrue(entities.isEmpty());

    }

    @Test
    public void shouldConvertFromListSubdocumentList() {
        DocumentEntity entity = createSubdocumentList();
        entityManager.insert(entity);

    }

    @Test
    public void shouldRetrieveListSubdocumentList() {
        DocumentEntity entity = entityManager.insert(createSubdocumentList());
        Document key = entity.find("_id").get();
        DocumentQuery query = select().from("AppointmentBook").where(key.getName()).eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).get();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of("AppointmentBook");
        List<List<Document>> documents = new ArrayList<>();

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", "EMAIL"),
                Document.of("information", "ada@lovelace.com")));

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", "MOBILE"),
                Document.of("information", "11 1231231 123")));

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", "PHONE"),
                Document.of("information", "phone")));

        entity.add(Document.of("contacts", documents));
        return entity;
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