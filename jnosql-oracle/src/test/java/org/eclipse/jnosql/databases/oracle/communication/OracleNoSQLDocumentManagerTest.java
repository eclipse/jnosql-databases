/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.document.DocumentDeleteQuery.delete;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class OracleNoSQLDocumentManagerTest {

    public static final String COLLECTION_NAME = "person";
    private static OracleNoSQLDocumentManager entityManager;

    @BeforeAll
    public static void setUp() throws IOException {
        entityManager = (OracleNoSQLDocumentManager) Database.INSTANCE.managerFactory().apply("database");
    }

    @BeforeEach
    void beforeEach() {
        DocumentDeleteQuery.delete().from(COLLECTION_NAME).delete(entityManager);
    }

    @Test
    void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertTrue(documentEntity.documents().stream().map(Document::name).anyMatch(s -> s.equals("_id")));
    }

    @Test
    void shouldThrowExceptionWhenInsertWithTTL() {
        var entity = getEntity();
        var ttl = Duration.ofSeconds(10);
        assertThrows(UnsupportedOperationException.class, () -> entityManager.insert(entity, ttl));
    }

    @Test
    void shouldUpdate() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").orElseThrow());
    }

    @Test
    void shouldRemoveEntity() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());

        Optional<Document> id = documentEntity.find("_id");
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.orElseThrow().get())
                .build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("_id")
                .eq(id.get().get())
                .build();

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).findAny().isEmpty());
    }

    @Test
    void shouldFindDocument() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find("_id");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.orElseThrow().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    void shouldFindDocument2() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find("_id");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .and("city").eq("Salvador")
                .and("_id").eq(id.orElseThrow().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).toList();
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    void shouldFindDocument3() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find("_id");
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .or("city").eq("Salvador")
                .and(id.orElseThrow().name()).eq(id.get().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).contains(entity);
    }

    @Test
    void shouldFindDocumentGreaterThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));
    }

    @Test
    void shouldFindDocumentGreaterEqualsThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gte(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));
    }

    @Test
    void shouldFindDocumentLesserThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false)
                .toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lt(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).toList();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(entitiesFound).hasSize(1);

            List<String> namesFound = entitiesFound.stream()
                    .flatMap(d -> d.find("name").stream())
                    .map(d-> d.get(String.class))
                    .toList();
            soft.assertThat(namesFound).contains("Lucas");
        });
    }

    @Test
    void shouldFindDocumentLesserEqualsThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lte(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).toList();
        assertEquals(2, entitiesFound.size());
    }

    @Test
    void shouldFindDocumentIn() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("location").in(asList("BR", "US"))
                .and("type").eq("V")
                .build();

        assertSoftly(soft -> {
            List<DocumentEntity> entitiesFound = entityManager.select(query).toList();
            soft.assertThat(entitiesFound).hasSize(entities.size());
            List<String> namesFound = entitiesFound.stream()
                    .flatMap(d -> d.find("name").stream())
                    .map(d-> d.get(String.class))
                    .toList();
            List<String> names = entities.stream()
                    .flatMap(d -> d.find("name").stream())
                    .map(d-> d.get(String.class))
                    .toList();
            soft.assertThat(namesFound).containsAll(names);
        });

    }

    @Test
    void shouldFindDocumentStart() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .skip(1L)
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(1, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));

        query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .skip(2L)
                .build();

        entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entitiesFound.isEmpty());

    }

    @Test
    void shouldFindDocumentLimit() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).toList();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .limit(1L)
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(1, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));

        query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .limit(2L)
                .build();

        entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());

    }

    @Test
    void shouldFindDocumentSort() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where("type").eq("V").build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .orderBy("age").asc()
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        List<Integer> ages = entitiesFound.stream()
                .map(e -> e.find("age").orElseThrow().get(Integer.class))
                .collect(Collectors.toList());

        assertThat(ages).contains(23, 25);

        query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .orderBy("age").desc()
                .build();

        entitiesFound = entityManager.select(query).toList();
        ages = entitiesFound.stream()
                .map(e -> e.find("age").orElseThrow().get(Integer.class))
                .collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(ages).contains(25, 23);

    }

    @Test
    void shouldFindAll() {
        entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = entityManager.select(query).toList();
        assertFalse(entities.isEmpty());
    }

    @Test
    void shouldDeleteAll() {
        entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        entities = entityManager.select(query).toList();
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldFindAllByFields() {
        entityManager.insert(getEntity());
        DocumentQuery query = select("name").from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = entityManager.select(query).toList();
        assertFalse(entities.isEmpty());
        final DocumentEntity entity = entities.get(0);
        assertSoftly(soft -> {
            soft.assertThat(entity.find("name")).isPresent();
            soft.assertThat(entity.find("_id")).isPresent();
            soft.assertThat(entity.find("city")).isNotPresent();
        });
    }


    @Test
    void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").orElseThrow();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq(id.get())
                .build();

        DocumentEntity entityFound = entityManager.select(query).toList().get(0);
        Document subDocument = entityFound.find("phones").orElseThrow();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"));
    }

    @Test
    void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").orElseThrow();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(id.name()).eq(id.get())
                .build();
        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").orElseThrow();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"),
                Document.of("mobile2", "1231231"));
    }

    @Test
    @Disabled
    void shouldCreateEntityByteArray() {
        byte[] contents = {1, 2, 3, 4, 5, 6};

        DocumentEntity entity = DocumentEntity.of("download");
        long id = ThreadLocalRandom.current().nextLong();
        entity.add("_id", id);
        entity.add("contents", contents);

        entityManager.insert(entity);

        List<DocumentEntity> entities = entityManager.select(select().from("download")
                .where("_id").eq(id).build()).collect(Collectors.toList());

        assertEquals(1, entities.size());
        DocumentEntity documentEntity = entities.get(0);
        assertEquals(id, documentEntity.find("_id").orElseThrow().get(Long.class));

        assertArrayEquals(contents, documentEntity.find("contents").orElseThrow().get(byte[].class));

    }

    @Test
    void shouldCreateDate() {
        LocalDate now = LocalDate.now();

        DocumentEntity entity = DocumentEntity.of("download");
        long id = ThreadLocalRandom.current().nextLong();
        entity.add("_id", id);
        entity.add("now", now);

        entityManager.insert(entity);

        List<DocumentEntity> entities = entityManager.select(select().from("download")
                .where("_id").eq(id).build()).collect(Collectors.toList());

        assertEquals(1, entities.size());
        DocumentEntity documentEntity = entities.get(0);
        assertSoftly(soft ->{
            soft.assertThat(id).isEqualTo(documentEntity.find("_id").orElseThrow().get(Long.class));
            soft.assertThat(now).isEqualTo(documentEntity.find("now").orElseThrow().get(LocalDate.class));
        });
    }

    @Test
    void shouldConvertFromListDocumentList() {
        DocumentEntity entity = createDocumentList();
        assertDoesNotThrow(() -> entityManager.insert(entity));
    }

    @Test
    void shouldRetrieveListDocumentList() {
        DocumentEntity entity = entityManager.insert(createDocumentList());
        Document key = entity.find("_id").orElseThrow();
        DocumentQuery query = select().from("AppointmentBook")
                .where(key.name())
                .eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).orElseThrow();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").orElseThrow().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    @Test
    void shouldCount() {
        entityManager.insert(getEntity());
        assertTrue(entityManager.count(COLLECTION_NAME) > 0);
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
        Document properties = documentEntity.find("properties").orElseThrow();
        Document document = properties.get(Document.class);
        assertThat(document).isNotNull().isEqualTo(Document.of("hallo", "Welt"));
    }

    @Test
    void shouldInsertNull() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("name", null));
        DocumentEntity documentEntity = entityManager.insert(entity);
        Optional<Document> name = documentEntity.find("name");
        assertSoftly(soft -> {
            soft.assertThat(name).isPresent();
            soft.assertThat(name).get().extracting(Document::name).isEqualTo("name");
            soft.assertThat(name).get().extracting(Document::get).isNull();
        });
    }

    @Test
    void shouldUpdateNull(){
        var entity = entityManager.insert(getEntity());
        entity.add(Document.of("name", null));
        var documentEntity = entityManager.update(entity);
        Optional<Document> name = documentEntity.find("name");
        assertSoftly(soft -> {
            soft.assertThat(name).isPresent();
            soft.assertThat(name).get().extracting(Document::name).isEqualTo("name");
            soft.assertThat(name).get().extracting(Document::get).isNull();
        });
    }

    @Test
    void shouldQuery(){
        entityManager.insert(getEntity());

        var query = "select * from database where database.content.name = 'Poliana'";
        Stream<DocumentEntity> entities = entityManager.sql(query);
        List<String> names = entities.map(d -> d.find("name").orElseThrow().get(String.class))
                .toList();
        assertThat(names).contains("Poliana");
    }

    @Test
    void shouldQueryParams(){
        entityManager.insert(getEntity());

        var query = "select * from database where database.content.name = ?";
        Stream<DocumentEntity> entities = entityManager.sql(query, "Poliana");
        List<String> names = entities.map(d -> d.find("name").orElseThrow().get(String.class))
                .toList();
        assertThat(names).contains("Poliana");
    }

    private DocumentEntity createDocumentList() {
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
        map.put("_id", UUID.randomUUID().toString());
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