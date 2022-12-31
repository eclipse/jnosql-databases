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
package org.eclipse.jnosql.communication.elasticsearch.document;

import co.elastic.clients.elasticsearch.core.SearchRequest;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.awaitility.Awaitility;
import org.eclipse.jnosql.communication.document.Documents;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticsearchDocumentManagerTest {

    private ElasticsearchDocumentManager entityManager;

    static {
        Awaitility.setDefaultPollDelay(100, MILLISECONDS);
        Awaitility.setDefaultTimeout(2L, SECONDS);
    }

    @NotNull
    private Callable<Long> numberOfEntitiesFrom(SearchRequest query) {
        return () -> entityManager.search(query).count();
    }

    @NotNull
    private Callable<Long> numberOfEntitiesFrom(QueryBuilder query) {
        return () -> entityManager.search(query).count();
    }

    @NotNull
    private Callable<Long> numberOfEntitiesFrom(DocumentQuery query) {
        return () -> entityManager.select(query).count();
    }

    @NotNull
    private Callable<DocumentEntity> getSingleResult(DocumentQuery query) {
        return () -> entityManager.singleResult(query).orElse(null);
    }

    @NotNull
    private Callable<List<DocumentEntity>> selectFrom(DocumentQuery query) {
        return () -> entityManager.select(query).collect(Collectors.toList());
    }

    @BeforeEach
    public void setUp() {

        ElasticsearchDocumentManagerFactory managerFactory = ElasticsearchDocumentManagerFactorySupplier.INSTANCE.get();
        entityManager = managerFactory.apply(DocumentEntityGerator.INDEX);

        DocumentDeleteQuery deleteQuery = DocumentDeleteQuery.delete().from(DocumentEntityGerator.COLLECTION_NAME).build();

        entityManager.delete(deleteQuery);

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(0L));
    }

    @Test
    public void shouldClose() {
        entityManager.close();
        assertThrows(RuntimeException.class,
                () -> entityManager.insert(DocumentEntityGerator.getEntity()));
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);

        Document id = documentEntity.find(ID_FIELD).get();
        DocumentQuery query = DocumentQuery.select()
                .from(documentEntity.getName())
                .where(id.getName()).eq(id.getValue())
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(getSingleResult(query), equalTo(documentEntity));
    }

    @Test
    public void shouldInsertTTL() {
        assertThrows(UnsupportedOperationException.class, () -> entityManager.insert(DocumentEntityGerator.getEntity(), Duration.ofSeconds(1L)));
    }

    @Test
    public void shouldReturnAll() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);

        DocumentQuery query = select().
                from(DocumentEntityGerator.COLLECTION_NAME)
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), greaterThan(0l));
    }


    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);

        Document id = entity.find(ID_FIELD).get();

        DocumentQuery query = select()
                .from(DocumentEntityGerator.COLLECTION_NAME)
                .where(id.getName())
                .eq(id.get())
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1L));

        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1L));
    }

    @Test
    public void shouldUserSearchRequest() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);

        var query = SearchRequest.of(b -> b
                .index(DocumentEntityGerator.COLLECTION_NAME)
                .query(q -> q.term(tq -> tq.field("name").value("Poliana"))));

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query),equalTo(1l));

        assertThat(entityManager.search(query)
                .collect(Collectors.toList())).contains(entity);

    }

    @Test
    public void shouldUserQueryBuilder() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);

        TermQueryBuilder query = termQuery("name", "Poliana");

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query),equalTo(1l));

        assertThat(entityManager.search(query)
                .collect(Collectors.toList())).contains(entity);
    }


    @Test
    public void shouldRemoveEntityByName() {
        DocumentEntity documentEntity = entityManager.insert(DocumentEntityGerator.getEntity());
        Document name = documentEntity.find("name").get();
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(name.getName()).eq(name.get()).build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1L));

        DocumentDeleteQuery deleteQuery = delete().from(DocumentEntityGerator.COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        entityManager.delete(deleteQuery);

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(0L));

    }

    @Test
    public void shouldRemoveEntityById() {
        DocumentEntity documentEntity = entityManager.insert(DocumentEntityGerator.getEntity());

        Document id = documentEntity.find("_id").get();
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1L));


        DocumentDeleteQuery deleteQuery = delete().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        entityManager.delete(deleteQuery);

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(0L));

    }

    @Test
    public void shouldFindDocumentByName() {
        DocumentEntity entity = entityManager.insert(DocumentEntityGerator.getEntity());
        Document name = entity.find("name").get();
        DocumentQuery query = select().
                from(DocumentEntityGerator.COLLECTION_NAME).
                where(name.getName())
                .eq(name.get())
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1l));

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        List<Document> names = entities.stream().map(e -> e.find("name").get())
                .distinct().collect(Collectors.toList());
        assertThat(names).contains(name);
    }

    @Test
    public void shouldFindDocumentById() {
        DocumentEntity entity = entityManager.insert(DocumentEntityGerator.getEntity());
        Document id = entity.find("_id").get();

        DocumentQuery query = select()
                .from(DocumentEntityGerator.COLLECTION_NAME)
                .where(id.getName()).eq(id.get())
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(selectFrom(query), contains(entity));

    }

    @Test
    public void shouldFindOrderByName() {
        final DocumentEntity poliana = DocumentEntityGerator.getEntity();
        final DocumentEntity otavio = DocumentEntityGerator.getEntity();
        poliana.add("name", "poliana");
        otavio.add("name", "otavio");
        otavio.add("_id", "id2");
        entityManager.insert(Arrays.asList(poliana, otavio));

        DocumentQuery query = DocumentQuery.select()
                .from(DocumentEntityGerator.COLLECTION_NAME)
                .orderBy("name").asc()
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(2L));

        String[] names = entityManager.select(query)
                .map(d -> d.find("name"))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(d -> d.get(String.class))
                .toArray(String[]::new);

        assertThat(names).containsExactly("otavio", "poliana");
    }

    @Test
    public void shouldFindOrderByNameDesc() {
        final DocumentEntity poliana = DocumentEntityGerator.getEntity();
        final DocumentEntity otavio = DocumentEntityGerator.getEntity();
        poliana.add("name", "poliana");
        otavio.add("name", "otavio");
        otavio.add("_id", "id2");
        entityManager.insert(Arrays.asList(poliana, otavio));

        DocumentQuery query = DocumentQuery.select()
                .from(DocumentEntityGerator.COLLECTION_NAME)
                .orderBy("name").desc()
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(2L));

        String[] names = entityManager.select(query)
                .map(d -> d.find("name"))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(d -> d.get(String.class))
                .toArray(String[]::new);

        assertThat(names).containsExactly("poliana", "otavio");
    }


    @Test
    public void shouldFindAll() {
        entityManager.insert(DocumentEntityGerator.getEntity());

        DocumentQuery query = select()
                .from(DocumentEntityGerator.COLLECTION_NAME)
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(selectFrom(query), hasSize(1));

    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1l));

        DocumentEntity entityFound = entityManager.singleResult(query).get();
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"));
    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();

        Document mobile = Document.of("mobile", "1231231");
        Document mobile2 = Document.of("mobile2", "1231231");

        entity.add(Document.of("phones", Arrays.
                asList(mobile, mobile2)));

        DocumentEntity entitySaved = entityManager.insert(entity);

        Document id = entitySaved.find("_id").get();
        DocumentQuery query = select().
                from(DocumentEntityGerator.COLLECTION_NAME).
                where(id.getName()).eq(id.get())
                .build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1l));

        DocumentEntity entityFound = entityManager.select(query)
                .collect(Collectors.toList())
                .get(0);

        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });

        assertThat(documents).contains(mobile, mobile2);
    }

    @Test
    public void shouldInsertAndRetrieveListSubdocumentList() {
        DocumentEntity entity = entityManager.insert(createSubdocumentList());

        Document key = entity.find("_id").get();
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(key.getName()).eq(key.get()).build();

        // it's required in order to avoid an eventual inconsistency
        await().until(numberOfEntitiesFrom(query), equalTo(1l));

        DocumentEntity documentEntity = entityManager.singleResult(query).orElse(null);
        assertNotNull(documentEntity);

        List<List<Document>> contacts = documentEntity.find("contacts")
                .orElseThrow()
                .get(List.class);

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    @Test
    public void shouldCount() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        DocumentEntity entity2 = DocumentEntityGerator.getEntity();
        entity2.add(Document.of("_id", "test"));
        entityManager.insert(entity);
        entityManager.insert(entity2);

        // it's required in order to avoid an eventual inconsistency
        await().until(() -> entityManager.count(DocumentEntityGerator.COLLECTION_NAME),
                equalTo(2l));
    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of(DocumentEntityGerator.COLLECTION_NAME);
        entity.add(Document.of("_id", "ids"));
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


}