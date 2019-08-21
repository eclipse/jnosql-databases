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
package org.jnosql.diana.elasticsearch.document;

import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.jnosql.diana.document.Documents;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.jnosql.diana.elasticsearch.document.DocumentEntityGerator.COLLECTION_NAME;
import static org.jnosql.diana.elasticsearch.document.DocumentEntityGerator.INDEX;
import static org.jnosql.diana.elasticsearch.document.DocumentEntityGerator.getEntity;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticsearchDocumentCollectionManagerTest {


    private ElasticsearchDocumentCollectionManager entityManager;

    @BeforeEach
    public void setUp() {
        ElasticsearchDocumentCollectionManagerFactory managerFactory = ElasticsearchDocumentCollectionManagerFactorySupplier.INSTACE.get();
        entityManager = managerFactory.get(INDEX);

    }

    @Test
    public void shouldClose() {
        entityManager.close();
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }

    @Test
    public void shouldInsertTTL() {
        assertThrows(UnsupportedOperationException.class, () -> {
            entityManager.insert(getEntity(), Duration.ofSeconds(1L));
        });
    }

    @Test
    public void shouldReturnAll() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> result = entityManager.select(query);
        assertFalse(result.isEmpty());
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
    public void shouldUserSearchBuilder() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        TermQueryBuilder query = termQuery("name", "Poliana");
        SECONDS.sleep(1L);
        List<DocumentEntity> account = entityManager.search(query, "person");
        assertFalse(account.isEmpty());
    }

    @Test
    public void shouldRemoveEntityByName() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.insert(getEntity());

        Document name = documentEntity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();

        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        SECONDS.sleep(1L);
        entityManager.delete(deleteQuery);
        SECONDS.sleep(1L);
        List<DocumentEntity> entities = entityManager.select(query);
        System.out.println(entities);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldRemoveEntityById() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        Document id = documentEntity.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldFindDocumentByName() throws InterruptedException {
        DocumentEntity entity = entityManager.insert(getEntity());
        Document name = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        SECONDS.sleep(1L);
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        List<Document> names = entities.stream().map(e -> e.find("name").get())
                .distinct().collect(Collectors.toList());
        assertThat(names, contains(name));
    }

    @Test
    public void shouldFindDocumentById() {
        DocumentEntity entity = entityManager.insert(getEntity());
        Document id = entity.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldFindAll() throws InterruptedException {
        entityManager.insert(getEntity());

        DocumentQuery query = select().from(COLLECTION_NAME).build();
        SECONDS.sleep(1L);
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();


        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.singleResult(query).get();
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
        Document id = entitySaved.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, containsInAnyOrder(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231")));
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
        DocumentQuery query = select().from(COLLECTION_NAME).where(key.getName()).eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).get();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    @Test
    public void shouldCount() throws InterruptedException {
        DocumentEntity entity = getEntity();
        DocumentEntity entity2 = getEntity();
        entity2.add(Document.of("_id", "test"));
        entityManager.insert(entity);
        entityManager.insert(entity2);
        SECONDS.sleep(1L);
        assertTrue(entityManager.count(COLLECTION_NAME) > 0);
    }


    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
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