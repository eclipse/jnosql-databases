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

package org.eclipse.jnosql.communication.solr.document;

import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.document.DocumentDeleteQuery.delete;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION_MATCHES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = INTEGRATION, matches = INTEGRATION_MATCHES)
public class DefaultSolrDocumentManagerTest {

    public static final String COLLECTION_NAME = "person";
    public static final String ID = "id";
    private static SolrDocumentManager entityManager;

    @BeforeAll
    public static void setUp() {
        entityManager = DocumentDatabase.INSTANCE.get();
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertTrue(documentEntity.documents().stream().map(Document::name).anyMatch(s -> s.equals(ID)));
    }

    @Test
    public void shouldThrowExceptionWhenInsertWithTTL() {
        assertThrows(UnsupportedOperationException.class, () -> entityManager.insert(getEntity(), Duration.ofSeconds(10)));
    }

    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldRemoveEntity() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());

        Optional<Document> id = documentEntity.find(ID);
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(ID).eq(id.get().get())
                .build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(ID)
                .eq(id.get().get())
                .build();

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find(ID);

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(ID).eq(id.get().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        final DocumentEntity result = entities.get(0);

        assertEquals(entity.find("name").get(), result.find("name").get());
        assertEquals(entity.find("city").get(), result.find("city").get());

    }


    @Test
    public void shouldFindDocument2() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find(ID);

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .and("city").eq("Salvador").and(ID).eq(id.get().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        final DocumentEntity result = entities.get(0);

        assertEquals(entity.find("name").get(), result.find("name").get());
        assertEquals(entity.find("city").get(), result.find("city").get());
    }

    @Test
    public void shouldFindDocument3() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Optional<Document> id = entity.find(ID);
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("Poliana")
                .or("city").eq("Salvador")
                .and(id.get().name()).eq(id.get().get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        final DocumentEntity result = entities.get(0);
        assertEquals(entity.find("name").get(), result.find("name").get());
        assertEquals(entity.find("city").get(), result.find("city").get());
    }

    @Test
    public void shouldFindDocumentGreaterThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(3, entitiesFound.size());
    }

    @Test
    public void shouldFindNot() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").not().eq("Lucas").build();
        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
    }

    @Test
    public void shouldFindDocumentGreaterEqualsThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gte(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));
    }

    @Test
    public void shouldFindDocumentLesserThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lt(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
    }

    @Test
    public void shouldFindDocumentLesserEqualsThan() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").lte(23)
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
    }

    @Test
    public void shouldFindDocumentLike() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entities = entityManager.insert(getEntitiesWithValues());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").like("Lu*")
                .and("type").eq("V")
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
    }

    @Test
    public void shouldFindDocumentIn() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("location").in(asList("BR", "US"))
                .and("type").eq("V")
                .build();

        assertEquals(3, entityManager.select(query).collect(Collectors.toList()).size());
    }

    @Test
    public void shouldFindDocumentStart() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .skip(1L)
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entitiesFound.size());
        assertThat(entitiesFound).isNotIn(entities.get(0));

        query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .skip(3L)
                .build();

        entitiesFound = entityManager.select(query).collect(Collectors.toList());
        assertTrue(entitiesFound.isEmpty());

    }

    @Test
    public void shouldFindDocumentLimit() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

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
        entityManager.delete(deleteQuery);
    }

    @Test
    public void shouldFindDocumentSort() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        Iterable<DocumentEntity> entitiesSaved = entityManager.insert(getEntitiesWithValues());
        List<DocumentEntity> entities = StreamSupport.stream(entitiesSaved.spliterator(), false).collect(Collectors.toList());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .orderBy("age").asc()
                .build();

        List<DocumentEntity> entitiesFound = entityManager.select(query).collect(Collectors.toList());
        List<Integer> ages = entitiesFound.stream()
                .map(e -> e.find("age").get().get(Integer.class))
                .collect(Collectors.toList());

        assertThat(ages).contains(22, 23, 25);

        query = select().from(COLLECTION_NAME)
                .where("age").gt(22)
                .and("type").eq("V")
                .orderBy("age").desc()
                .build();

        entitiesFound = entityManager.select(query).collect(Collectors.toList());
        ages = entitiesFound.stream()
                .map(e -> e.find("age").get().get(Integer.class))
                .collect(Collectors.toList());
        assertThat(ages).contains(25, 23, 22);

    }

    @Test
    public void shouldExecuteNativeQuery() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());

        List<DocumentEntity> entitiesFound = entityManager.solr("age:22 AND type:V AND _entity:person");
        assertEquals(1, entitiesFound.size());
    }

    @Test
    public void shouldExecuteNativeQueryParams() {
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());

        Map<String, Object> params = new HashMap<>();
        params.put("age", 22);
        params.put("type", "V");
        params.put("entity", "person");

        List<DocumentEntity> entitiesFound = entityManager.solr("age:@age AND type:@type AND _entity:@entity"
                , params);
        assertEquals(1, entitiesFound.size());
    }

    @Test
    public void shouldExecuteNativeQueryParamsReplaceAll() {
        entityManager.insert(getEntitiesWithValues());
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).build();
        entityManager.delete(deleteQuery);
        entityManager.insert(getEntitiesWithValues());

        Map<String, Object> params = new HashMap<>();
        params.put("age", 22);

        List<DocumentEntity> entitiesFound = entityManager.solr("age:@age AND age:@age"
                , params);
        assertEquals(1, entitiesFound.size());
    }


    @Test
    public void shouldFindAll() {
        entityManager.insert(getEntity());
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());;
        assertFalse(entities.isEmpty());
    }


    @Test
    public void shouldReturnErrorWhenSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        Assertions.assertThrows(SolrException.class, () -> entityManager.insert(entity));

    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        Assertions.assertThrows(SolrException.class, () -> entityManager.insert(entity));
    }

    @Test
    public void shouldCreateDate() {
        Date date = new Date();
        LocalDate now = LocalDate.now();

        DocumentEntity entity = DocumentEntity.of("download");
        long id = ThreadLocalRandom.current().nextLong(1, 10);
        entity.add(ID, id);
        entity.add("date", date);
        entity.add("now", now);

        entityManager.insert(entity);

        List<DocumentEntity> entities = entityManager.select(select().from("download")
                .where(ID).eq(id).build()).collect(Collectors.toList());;

        assertEquals(1, entities.size());
        DocumentEntity documentEntity = entities.get(0);
        assertEquals(date, documentEntity.find("date").get().get(Date.class));
        assertEquals(now, documentEntity.find("date").get().get(LocalDate.class));
    }

    @Test
    public void shouldRetrieveListSubdocumentList() {
        Assertions.assertThrows(SolrException.class, () -> entityManager.insert(createSubdocumentList()));
    }

    @Test
    public void shouldCount() {
        DocumentEntity entity = entityManager.insert(getEntity());
        assertTrue(entityManager.count(COLLECTION_NAME) > 0);
    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of("AppointmentBook");
        entity.add(Document.of(ID, new Random().nextInt()));
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
        map.put(ID, ThreadLocalRandom.current().nextLong(1, 10));
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