/*
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
 */

package org.eclipse.jnosql.communication.ravendb.document;

import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.eclipse.jnosql.communication.document.DocumentDeleteQuery.delete;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
public class RavenDBDocumentManagerTest {

    public static final String COLLECTION_NAME = "person";
    private static final long TIME_LIMIT = 500L;
    private static final String APPOINTMENT_BOOK = "AppointmentBook";
    private static DocumentManager manager;

    @BeforeAll
    public static void setUp() throws IOException {
        manager = DocumentConfigurationUtils.INSTANCE.get().apply("database");
    }

    @BeforeEach
    public void before() {
        manager.delete(delete().from(COLLECTION_NAME).build());
        manager.delete(delete().from(APPOINTMENT_BOOK).build());
    }

    @AfterEach
    public void after() {
        manager.delete(delete().from(COLLECTION_NAME).build());
        manager.delete(delete().from(APPOINTMENT_BOOK).build());
    }


    @Test
    public void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = manager.insert(entity);
        assertTrue(documentEntity.documents().stream().map(Document::name).anyMatch(s -> s.equals("_id")));
    }

    @Test
    public void shouldThrowExceptionWhenInsertWithTTL() {
        DocumentEntity entity = manager.insert(getEntity(), Duration.ofMillis(1));
        Optional<Document> id = entity.find("_id");
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get().get())
                .build();

    }

    @Test
    public void shouldUpdate() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = manager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = manager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldRemoveEntity() throws InterruptedException {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = manager.insert(entity);

        Optional<Document> id = documentEntity.find("_id");
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get().get())
                .build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("_id")
                .eq(id.get().get())
                .build();

        manager.delete(deleteQuery);
        assertTrue(manager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = manager.insert(getEntity());
        Optional<Document> id = entity.find("_id");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get().get())
                .build();

        List<DocumentEntity> entities = manager.select(query)
                .collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    public void shouldRunSingleResult() {
        DocumentEntity entity = manager.insert(getEntity());
        Optional<Document> id = entity.find("_id");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get().get())
                .build();

        Optional<DocumentEntity> result = manager.singleResult(query);
        assertTrue(result.isPresent());
        assertEquals(entity, result.get());
    }

    @Test
    public void shouldFindDocument2() {
        DocumentEntity entity = manager.insert(getEntity());
        Optional<Document> id = entity.find("_id");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .and("city").eq("Salvador").and("_id").eq(id.get().get())
                .build();

        List<DocumentEntity> entities = manager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    public void shouldFindDocument3() {
        DocumentEntity entity = manager.insert(getEntity());
        Optional<Document> id = entity.find("_id");
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .or("city").eq("Salvador")
                .and(id.get().name()).eq(id.get().get())
                .build();

        List<DocumentEntity> entities = manager.select(query)
                .collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    public void shouldFindDocumentGreaterThan() throws InterruptedException {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        manager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = manager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .build();

        Thread.sleep(TIME_LIMIT);
        List<DocumentEntity> entitiesFound = manager.select(query).collect(Collectors.toList());
        assertThat(entitiesFound).hasSize(2).isNotIn(entities.get(0));
    }

    @Test
    public void shouldFindDocumentGreaterEqualsThan() throws InterruptedException {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        manager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = manager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gte(23)
                .and("type").eq("V")
                .build();

        Thread.sleep(TIME_LIMIT);
        List<DocumentEntity> entitiesFound = manager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));
    }

    @Test
    public void shouldFindDocumentLesserThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        manager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = manager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lt(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = manager.select(query).collect(Collectors.toList());
        assertThat(entitiesFound).hasSize(1).contains(entities.get(0));
    }

    @Test
    public void shouldFindDocumentLesserEqualsThan() throws InterruptedException {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        manager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = manager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lte(23)
                .and("type").eq("V")
                .build();

        Thread.sleep(TIME_LIMIT);
        List<DocumentEntity> entitiesFound = manager.select(query).collect(Collectors.toList());
        System.out.println(entitiesFound);
        assertThat(entitiesFound)
                .hasSize(2)
                .contains(entities.get(0), entities.get(2));
    }


    @Test
    public void shouldFindDocumentIn() throws InterruptedException {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        manager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = manager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("location").in(asList("BR", "US"))
                .and("type").eq("V")
                .build();
        Thread.sleep(TIME_LIMIT);
        assertEquals(entities, manager.select(query).collect(Collectors.toList()));
    }

    @Test
    public void shouldFindAll() {
        manager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = manager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = manager.insert(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get())
                .build();

        DocumentEntity entityFound = manager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"));
    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = manager.insert(entity);
        Document id = entitySaved.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(id.name()).eq(id.get())
                .build();
        DocumentEntity entityFound = manager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"),
                Document.of("mobile2", "1231231"));
    }

    @Test
    public void shouldConvertFromListSubdocumentList() {
        DocumentEntity entity = createSubdocumentList();
        manager.insert(entity);

    }

    @Test
    public void shouldRetrieveListSubdocumentList() {
        DocumentEntity entity = manager.insert(createSubdocumentList());
        Document key = entity.find("_id").get();
        DocumentQuery query = select().from(APPOINTMENT_BOOK)
                .where(key.name())
                .eq(key.get()).build();

        DocumentEntity documentEntity = manager.singleResult(query).get();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of(APPOINTMENT_BOOK);
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


    @Test
    public void shouldCount() {
        DocumentEntity entity = getEntity();
        manager.insert(entity);
        assertTrue(manager.count(COLLECTION_NAME) > 0);
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

    private List<DocumentEntity> getEntitiesWithValues() {
        DocumentEntity lucas = DocumentEntity.of(COLLECTION_NAME);
        lucas.add(Document.of("name", "Lucas"));
        lucas.add(Document.of("age", 22));
        lucas.add(Document.of("location", "BR"));
        lucas.add(Document.of("type", "V"));

        DocumentEntity otavio = DocumentEntity.of(COLLECTION_NAME);
        otavio.add(Document.of("name", "Otavio"));
        otavio.add(Document.of("age", 25));
        otavio.add(Document.of("location", "BR"));
        otavio.add(Document.of("type", "V"));

        DocumentEntity luna = DocumentEntity.of(COLLECTION_NAME);
        luna.add(Document.of("name", "Luna"));
        luna.add(Document.of("age", 23));
        luna.add(Document.of("location", "US"));
        luna.add(Document.of("type", "V"));

        return asList(lucas, otavio, luna);
    }

}
