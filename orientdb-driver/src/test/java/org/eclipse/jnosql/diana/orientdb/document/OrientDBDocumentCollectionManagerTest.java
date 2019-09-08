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
 *   Lucas Furlaneto
 */
package org.eclipse.jnosql.diana.orientdb.document;

import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.jnosql.diana.document.Documents;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonMap;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.eclipse.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OrientDBDocumentCollectionManagerTest {
    public static final String COLLECTION_NAME = "person";

    private OrientDBDocumentCollectionManager entityManager;

    @BeforeEach
    public void setUp() {
        entityManager = DocumentConfigurationUtils.get().get(Database.DATABASE);
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertNotNull(documentEntity);
        Optional<Document> document = documentEntity.find(RID_FIELD);
        assertTrue(document.isPresent());

    }

    @Test
    public void shouldThrowExceptionWhenSaveWithTTL() {
        assertThrows(UnsupportedOperationException.class, () -> entityManager.insert(getEntity(), Duration.ZERO));
    }

    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManager.update(entity);

        Document id = entity.find(OrientDBConverter.RID_FIELD).get();
        DocumentQuery query = select().from(entity.getName())
                .where(id.getName()).eq(id.get())
                .build();
        Optional<DocumentEntity> updated = entityManager.singleResult(query);

        assertTrue(updated.isPresent());
        assertEquals(newField, updated.get().find(newField.getName()).get());
    }

    @Test
    public void shouldUpdateWithRetry() {
        DocumentEntity entity = entityManager.insert(getEntity());
        entity.add(Document.of(OrientDBConverter.VERSION_FIELD, 0));
        Document newField = Documents.of("newField", "99");
        entity.add(newField);
        entityManager.update(entity);

        Document id = entity.find(OrientDBConverter.RID_FIELD).get();
        DocumentQuery query = select().from(entity.getName())
                .where(id.getName()).eq(id.get())
                .build();
        Optional<DocumentEntity> updated = entityManager.singleResult(query);

        assertTrue(updated.isPresent());
        assertEquals(newField, updated.get().find(newField.getName()).get());
    }

    @Test
    public void shouldRemoveEntity() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());

        Document id = documentEntity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Document id = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldSQL() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find("name");

        List<DocumentEntity> entities = entityManager.sql("select * from person where name = ?", id.get().get())
                .collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldSQL2() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find("name");

        List<DocumentEntity> entities = entityManager.sql("select * from person where name = :name",
                singletonMap("name", id.get().get())).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
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
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
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


        DocumentQuery query = select().from(COLLECTION_NAME).where("name").eq("Poliana")
                .and("age").gte(10).build();

        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("name").eq("Poliana")
                .and("age").gte(10).build();

        assertFalse(entityManager.select(query).collect(Collectors.toList()).isEmpty());

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldQueryOr() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("age", 24));
        entityManager.insert(entity);


        DocumentQuery query = select().from(COLLECTION_NAME).where("name").eq("Poliana")
                .or("age").gte(10).build();

        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("name").eq("Poliana")
                .or("age").gte(10).build();

        assertFalse(entityManager.select(query).collect(Collectors.toList()).isEmpty());

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldQueryGreaterThan() {
        DocumentEntity entity = getEntity();
        entity.add("age", 25);
        entityManager.insert(entity);

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(25)
                .build();
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());

        DocumentQuery query2 = select().from(COLLECTION_NAME)
                .where("age").gt(24)
                .build();
        assertTrue(entityManager.select(query2).collect(Collectors.toList()).size() == 1);
    }

    @Test
    public void shouldQueryLesserThan() {
        DocumentEntity entity = getEntity();
        entity.add("age", 25);
        entityManager.insert(entity);

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lt(25)
                .build();
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());

        DocumentQuery query2 = select().from(COLLECTION_NAME)
                .where("age").lt(26)
                .build();
        assertTrue(entityManager.select(query2).collect(Collectors.toList()).size() == 1);
    }

    @Test
    public void shouldQueryLesserEqualsThan() {
        DocumentEntity entity = getEntity();
        entity.add("age", 25);
        entityManager.insert(entity);

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lte(24)
                .build();
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());

        DocumentQuery query2 = select().from(COLLECTION_NAME)
                .where("age").lte(25)
                .build();
        assertTrue(entityManager.select(query2).collect(Collectors.toList()).size() == 1);

        DocumentQuery query3 = select().from(COLLECTION_NAME)
                .where("age").lte(26)
                .build();
        assertTrue(entityManager.select(query3).collect(Collectors.toList()).size() == 1);
    }

    @Test
    public void shouldQueryIn() {
        entityManager.insert(getEntities());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("city").in(asList("Salvador", "Assis"))
                .build();
        assertTrue(entityManager.select(query).collect(Collectors.toList()).size() == 2);

        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME)
                .where("city").in(asList("Salvador", "Assis", "Sao Paulo"))
                .build();
        entityManager.delete(deleteQuery);

        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldQueryLike() {
        List<DocumentEntity> entitiesSaved = StreamSupport.stream(entityManager.insert(getEntities()).spliterator(), false)
                .collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("city").like("Sa%")
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.size() == 2);
        assertThat(entities, containsInAnyOrder(entitiesSaved.get(0), entitiesSaved.get(1)));
    }

    @Test
    public void shouldQueryNot() {
        List<DocumentEntity> entitiesSaved = StreamSupport.stream(entityManager.insert(getEntities()).spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("city").not().eq("Assis")
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.size() == 2);
        assertThat(entities, containsInAnyOrder(entitiesSaved.get(0), entitiesSaved.get(1)));
    }

    @Test
    public void shouldQueryStart() {
        entityManager.insert(getEntities());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .skip(1)
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.size() == 2);
    }

    @Test
    public void shouldQueryLimit() {
        entityManager.insert(getEntities());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .limit(2)
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entities.size() == 2);
    }

    @Test
    public void shouldQueryOrderBy() {
        List<DocumentEntity> entitiesSaved = StreamSupport.stream(entityManager.insert(getEntities()).spliterator(), false).collect(Collectors.toList());

        DocumentQuery queryAsc = select().from(COLLECTION_NAME)
                .orderBy("name").asc()
                .build();

        List<DocumentEntity> entitiesAsc = entityManager.select(queryAsc).collect(Collectors.toList());
        assertThat(entitiesAsc, contains(entitiesSaved.get(2), entitiesSaved.get(1), entitiesSaved.get(0)));

        DocumentQuery queryDesc = select().from(COLLECTION_NAME)
                .orderBy("name").desc()
                .build();

        List<DocumentEntity> entitiesDesc = entityManager.select(queryDesc).collect(Collectors.toList());
        assertThat(entitiesDesc, contains(entitiesSaved.get(0), entitiesSaved.get(1), entitiesSaved.get(2)));
    }

    @Test
    public void shouldQueryMultiOrderBy() {
        List<DocumentEntity> entities = new ArrayList<>(getEntities());
        DocumentEntity bruno = DocumentEntity.of(COLLECTION_NAME);
        bruno.add(Document.of("name", "Bruno"));
        bruno.add(Document.of("city", "Sao Paulo"));
        entities.add(bruno);

        List<DocumentEntity> entitiesSaved = StreamSupport.stream(entityManager.insert(entities).spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .orderBy("city").desc()
                .orderBy("name").asc()
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertThat(entitiesFound, contains(entitiesSaved.get(3), entitiesSaved.get(1), entitiesSaved.get(0), entitiesSaved.get(2)));
    }

    @Test
    public void shouldLive() {
        AtomicBoolean condition = new AtomicBoolean(false);
        List<DocumentEntity> entities = new ArrayList<>();
        OrientDBLiveCreateCallback<DocumentEntity> callback = d -> {
            entities.add(d);
            condition.set(true);
        };

        entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();

        entityManager.live(query, OrientDBLiveCallbackBuilder.builder().onCreate(callback).build());
        entityManager.insert(getEntity());
        await().untilTrue(condition);
        assertFalse(entities.isEmpty());
    }

    @Test
    @Disabled
    public void shouldLiveUpdateCallback() {

        AtomicBoolean condition = new AtomicBoolean(false);
        List<DocumentEntity> entities = new ArrayList<>();
        OrientDBLiveUpdateCallback<DocumentEntity> callback = d -> {
            entities.add(d);
            condition.set(true);
        };

        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();

        entityManager.live(query, OrientDBLiveCallbackBuilder.builder().onUpdate(callback).build());
        Document newName = Document.of("name", "Lucas");
        entity.add(newName);
        entityManager.update(entity);
        await().untilTrue(condition);
        assertFalse(entities.isEmpty());
        assertFalse(entities.isEmpty());
    }

    @Test
    @Disabled
    public void shouldLiveDeleteCallback() {
        AtomicBoolean condition = new AtomicBoolean(false);
        OrientDBLiveDeleteCallback<DocumentEntity> callback = d -> condition.set(true);
        entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();

        entityManager.live(query, OrientDBLiveCallbackBuilder.builder().onDelete(callback).build());
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        await().untilTrue(condition);
    }

    @Test
    public void shouldLiveWithNativeQuery() {
        AtomicBoolean condition = new AtomicBoolean(false);
        List<DocumentEntity> entities = new ArrayList<>();
        OrientDBLiveCreateCallback<DocumentEntity> callback = d -> {
            entities.add(d);
            condition.set(true);
        };

        entityManager.insert(getEntity());

        entityManager.live("SELECT FROM person", OrientDBLiveCallbackBuilder.builder().onCreate(callback).build());
        entityManager.insert(getEntity());
        await().untilTrue(condition);
        assertFalse(entities.isEmpty());
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

    @Test
    public void shouldCount() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertNotNull(documentEntity);
        assertTrue(entityManager.count(COLLECTION_NAME) > 0);

    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of("AppointmentBook");
        entity.add(Document.of("_id", new Random().nextInt()));
        List<List<Document>> documents = new ArrayList<>();

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", ContactType.EMAIL),
                Document.of("information", "ada@lovelace.com")));

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", ContactType.MOBILE),
                Document.of("information", "11 1231231 123")));

        documents.add(asList(Document.of("name", "Ada"), Document.of("type", ContactType.PHONE),
                Document.of("information", "phone")));

        entity.add(Document.of("contacts", documents));
        return entity;
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

    private List<DocumentEntity> getEntities() {
        DocumentEntity otavio = DocumentEntity.of(COLLECTION_NAME);
        otavio.add(Document.of("name", "Otavio"));
        otavio.add(Document.of("city", "Sao Paulo"));

        DocumentEntity lucas = DocumentEntity.of(COLLECTION_NAME);
        lucas.add(Document.of("name", "Lucas"));
        lucas.add(Document.of("city", "Assis"));

        return asList(getEntity(), otavio, lucas);
    }

    @AfterEach
    void removePersons() {
        entityManager.insert(getEntity());
        DocumentDeleteQuery query = delete().from(COLLECTION_NAME).build();
        entityManager.delete(query);
    }
}