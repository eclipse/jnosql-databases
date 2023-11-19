/*
 *
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.couchdb.communication;

import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.eclipse.jnosql.databases.couchdb.communication.configuration.DocumentDatabase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.document.DocumentDeleteQuery.delete;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DefaultCouchDBDocumentManagerTest {

    public static final String COLLECTION_NAME = "person";

    private CouchDBDocumentManager entityManager;

    {
        CouchDBDocumentManagerFactory managerFactory = DocumentDatabase.INSTANCE.get();
        entityManager = managerFactory.apply("people");
    }

    @BeforeEach
    void setUp() {
        DocumentDeleteQuery query = delete().from(COLLECTION_NAME).build();
        entityManager.delete(query);
    }

    @Test
    void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }

    @Test
    void shouldInsertNotId() {
        DocumentEntity entity = getEntity();
        entity.remove(CouchDBConstant.ID);
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertTrue(documentEntity.find(CouchDBConstant.ID).isPresent());
    }

    @Test
    void shouldUpdate() {
        DocumentEntity entity = getEntity();
        entity.remove(CouchDBConstant.ID);
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    void shouldReturnErrorOnUpdate() {
        assertThrows(NullPointerException.class, () -> entityManager.update((DocumentEntity) null));
        assertThrows(CouchDBHttpClientException.class, () -> {
            DocumentEntity entity = getEntity();
            entity.remove(CouchDBConstant.ID);
            entityManager.update(entity);

        });

        assertThrows(CouchDBHttpClientException.class, () -> {
            DocumentEntity entity = getEntity();
            entity.add(CouchDBConstant.ID, "not_found");
            entityManager.update(entity);

        });
    }


    @Test
    void shouldSelect() {
        DocumentEntity entity = getEntity();
        entity.remove(CouchDBConstant.ID);
        entity = entityManager.insert(entity);
        Object id = entity.find(CouchDBConstant.ID).map(Document::get).get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(CouchDBConstant.ID).eq(id).build();
        DocumentEntity documentFound = entityManager.singleResult(query).get();
        assertEquals(entity, documentFound);
    }

    @Test
    void shouldSelectEmptyResult() {
        DocumentQuery query = select().from(COLLECTION_NAME).where("no_field").eq("not_found").build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldRemoveEntityByName() {
        DocumentEntity entity = getEntity();
        entity.remove(CouchDBConstant.ID);
        entity = entityManager.insert(entity);

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.name()).eq(name.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME)
                .where(name.name()).eq(name.get()).build();
        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    void shouldCount() {
        DocumentEntity entity = getEntity();
        entity.remove(CouchDBConstant.ID);
        entityManager.insert(entity);
        long count = entityManager.count();
        assertTrue(count > 0);
    }

    @Test
    void shouldSelectWithCouchDBDocumentQuery() {

        for (int index = 0; index < 4; index++) {
            DocumentEntity entity = getEntity();
            entity.remove(CouchDBConstant.ID);
            entity.add("index", index);
            entityManager.insert(entity);
        }
        CouchDBDocumentQuery query = CouchDBDocumentQuery.of(select().from(COLLECTION_NAME)
                .where("index").in(asList(0, 1, 2, 3, 4)).limit(2).build());

        assertFalse(query.getBookmark().isPresent());
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());
        assertTrue(query.getBookmark().isPresent());
        String bookmark = query.getBookmark().get();

        entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());
        assertTrue(query.getBookmark().isPresent());
        assertNotEquals(bookmark, query.getBookmark().get());

        entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldExecuteInStringQueryAtCouchbase() {
        for (int index = 0; index < 4; index++) {
            DocumentEntity entity = getEntity();
            entity.remove(CouchDBConstant.ID);
            entity.add("index", index);
            entityManager.insert(entity);
        }
        CouchDBDocumentQuery query = CouchDBDocumentQuery.of(select().from(COLLECTION_NAME)
                .where("name").in(Arrays.asList("Poliana", "Poliana")).build());
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(4, entities.size());
    }

    @Test
    void shouldConvertFromListSubdocumentList() {
        DocumentEntity entity = createDocumentList();
        entityManager.insert(entity);

    }

    @Test
    void shouldRetrieveListDocumentList() {
        DocumentEntity entity = entityManager.insert(createDocumentList());
        Document key = entity.find(CouchDBConstant.ID).get();
        DocumentQuery query = select().from("AppointmentBook").where(key.name()).eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).get();
        assertNotNull(documentEntity);
        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();
        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    @Test
    void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get())
                .build();

        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"));
    }

    @Test
    void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"),
                Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(id.name()).eq(id.get())
                .build();
        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"),
                Document.of("mobile2", "1231231"));
    }

    @Test
    void shouldSaveMap() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        String id = UUID.randomUUID().toString();
        entity.add("properties", Collections.singletonMap("hallo", "Welt"));
        entity.add("scope", "xxx");
        entity.add("_id", id);
        entityManager.insert(entity);
        final DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id).and("scope").eq("xxx").build();
        final Optional<DocumentEntity> optional = entityManager.select(query).findFirst();
        Assertions.assertTrue(optional.isPresent());
        DocumentEntity documentEntity = optional.get();
        Document properties = documentEntity.find("properties").get();
        Assertions.assertNotNull(properties);
    }

    private DocumentEntity createDocumentList() {
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
        map.put(CouchDBConstant.ID, "id");

        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}