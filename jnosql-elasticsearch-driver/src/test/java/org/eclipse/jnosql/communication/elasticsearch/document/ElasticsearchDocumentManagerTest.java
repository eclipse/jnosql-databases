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

import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ElasticsearchDocumentManagerTest {


    private ElasticsearchDocumentManager entityManager;

    @BeforeEach
    public void setUp() {
        ElasticsearchDocumentManagerFactory managerFactory = ElasticsearchDocumentCollectionManagerFactorySupplier.INSTANCE.get();
        entityManager = managerFactory.apply(DocumentEntityGerator.INDEX);

        DocumentDeleteQuery deleteQuery = DocumentDeleteQuery.delete().from("person").build();

        entityManager.delete(deleteQuery);

    }

    @Test
    public void shouldClose() {
        entityManager.close();
    }

    @Test
    public void shouldInsert() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }

    @Test
    public void shouldInsertTTL() {
        assertThrows(UnsupportedOperationException.class, () -> entityManager.insert(DocumentEntityGerator.getEntity(), Duration.ofSeconds(1L)));
    }

    @Test
    public void shouldReturnAll() throws InterruptedException {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).build();
        SECONDS.sleep(1L);
        List<DocumentEntity> result = entityManager.select(query).collect(Collectors.toList());
        assertFalse(result.isEmpty());
    }

    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldUserSearchBuilder() throws InterruptedException {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entityManager.insert(entity);
        TermQueryBuilder query = termQuery("name", "Poliana");
        SECONDS.sleep(1L);
        List<DocumentEntity> account = entityManager.search(query).collect(Collectors.toList());
        assertFalse(account.isEmpty());
    }

    @Test
    public void shouldRemoveEntityByName() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.insert(DocumentEntityGerator.getEntity());

        Document name = documentEntity.find("name").get();
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(name.getName()).eq(name.get()).build();

        DocumentDeleteQuery deleteQuery = delete().from(DocumentEntityGerator.COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        SECONDS.sleep(1L);
        entityManager.delete(deleteQuery);
        SECONDS.sleep(1L);
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        System.out.println(entities);
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldRemoveEntityById() {
        DocumentEntity documentEntity = entityManager.insert(DocumentEntityGerator.getEntity());
        Document id = documentEntity.find("_id").get();

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        DocumentDeleteQuery deleteQuery = delete().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).collect(Collectors.toList()).isEmpty());
    }

    @Test
    public void shouldFindDocumentByName() throws InterruptedException {
        DocumentEntity entity = entityManager.insert(DocumentEntityGerator.getEntity());
        Document name = entity.find("name").get();

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        SECONDS.sleep(1L);
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

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        entity.remove(EntityConverter.ENTITY);
        entities.get(0).remove(EntityConverter.ENTITY);
        assertThat(entities).contains(entity);
    }

    @Test
    public void shouldFindOrderByName() throws InterruptedException {
        final DocumentEntity poliana = DocumentEntityGerator.getEntity();
        final DocumentEntity otavio = DocumentEntityGerator.getEntity();
        poliana.add("name", "poliana");
        otavio.add("name", "otavio");
        otavio.add("_id", "id2");
        entityManager.insert(Arrays.asList(poliana, otavio));
        SECONDS.sleep(1L);
        DocumentQuery query = DocumentQuery.select().from("person").orderBy("name").asc().build();
        String[] names = entityManager.select(query).map(d -> d.find("name"))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(d -> d.get(String.class))
                .toArray(String[]::new);

        assertArrayEquals(names, new String[]{"otavio", "poliana"});
    }

    @Test
    public void shouldFindOrderByNameDesc() throws InterruptedException {
        final DocumentEntity poliana = DocumentEntityGerator.getEntity();
        final DocumentEntity otavio = DocumentEntityGerator.getEntity();
        poliana.add("name", "poliana");
        otavio.add("name", "otavio");
        otavio.add("_id", "id2");
        entityManager.insert(Arrays.asList(poliana, otavio));
        SECONDS.sleep(1L);
        DocumentQuery query = DocumentQuery.select().from("person").orderBy("name").desc().build();
        String[] names = entityManager.select(query).map(d -> d.find("name"))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(d -> d.get(String.class))
                .toArray(String[]::new);

        assertArrayEquals(names, new String[]{"poliana", "otavio"});
    }


    @Test
    public void shouldFindAll() throws InterruptedException {
        entityManager.insert(DocumentEntityGerator.getEntity());

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).build();
        SECONDS.sleep(1L);
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();


        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.singleResult(query).get();
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"));
    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        entity.add(Document.of("phones", Arrays.asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();

        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).collect(Collectors.toList()).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<>() {
        });
        assertThat(documents).contains(Document.of("mobile", "1231231"),
                Document.of("mobile2", "1231231"));
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
        DocumentQuery query = select().from(DocumentEntityGerator.COLLECTION_NAME).where(key.getName()).eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).get();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    @Test
    public void shouldCount() throws InterruptedException {
        DocumentEntity entity = DocumentEntityGerator.getEntity();
        DocumentEntity entity2 = DocumentEntityGerator.getEntity();
        entity2.add(Document.of("_id", "test"));
        entityManager.insert(entity);
        entityManager.insert(entity2);
        SECONDS.sleep(1L);
        assertTrue(entityManager.count(DocumentEntityGerator.COLLECTION_NAME) > 0);
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