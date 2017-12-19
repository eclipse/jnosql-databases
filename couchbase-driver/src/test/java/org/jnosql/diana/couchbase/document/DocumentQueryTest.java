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
package org.jnosql.diana.couchbase.document;


import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.couchbase.key.CouchbaseKeyValueConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DocumentQueryTest {

    public static final String COLLECTION_NAME = "person";
    private CouchbaseDocumentCollectionManager entityManager;

    {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManager = managerFactory.get("default");
    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager("default");
        keyValueEntityManager.remove("person:id");
        keyValueEntityManager.remove("person:id2");
        keyValueEntityManager.remove("person:id3");
        keyValueEntityManager.remove("person:id4");
    }

    @BeforeClass
    public static void beforeClass() throws InterruptedException {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        CouchbaseDocumentCollectionManager entityManager = managerFactory.get("default");

        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));
        DocumentEntity entity2 = DocumentEntity.of("person", asList(Document.of("_id", "id2")
                , Document.of("name", "name")));
        DocumentEntity entity3 = DocumentEntity.of("person", asList(Document.of("_id", "id3")
                , Document.of("name", "name")));
        DocumentEntity entity4 = DocumentEntity.of("person", asList(Document.of("_id", "id4")
                , Document.of("name", "name3")));

        entityManager.insert(Arrays.asList(entity, entity2, entity3, entity4));
        Thread.sleep(2_000L);

    }


    @Test
    public void shouldShouldDefineLimit() {

        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));

        Document name = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get())
                .limit(2L)
                .build();

        List<DocumentEntity> entities = entityManager.select(query);
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineStart() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));

        Document name = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get())
                .start(1L)
                .build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineLimitAndStart() {

        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get())
                .start(2L)
                .limit(2L)
                .build();

        List<DocumentEntity> entities = entityManager.select(query);
        assertEquals(1, entities.size());

    }


    @Test
    public void shouldSelectAll() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));


        DocumentQuery query = select().from(COLLECTION_NAME).build();
        Optional<Document> name = entity.find("name");
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertTrue(entities.size() >= 4);
    }


    @Test
    public void shouldFindDocumentByName() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id4")
                , Document.of("name", "name3"), Document.of("_key", "person:id4")));

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldFindDocumentByNameSortAsc() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id4")
                , Document.of("name", "name3"), Document.of("_key", "person:id4")));

        Optional<Document> name = entity.find("name");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .orderBy("name").asc()
                .build();

        List<DocumentEntity> entities = entityManager.select(query);
        List<String> result = entities.stream().flatMap(e -> e.getDocuments().stream())
                .filter(d -> "name".equals(d.getName()))
                .map(d -> d.get(String.class))
                .collect(Collectors.toList());

        assertFalse(result.isEmpty());
        assertThat(result, contains("name", "name", "name", "name3"));
    }

    @Test
    public void shouldFindDocumentByNameSortDesc() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id4")
                , Document.of("name", "name3"), Document.of("_key", "person:id4")));

        Optional<Document> name = entity.find("name");

        DocumentQuery query = select().from(COLLECTION_NAME)
                .orderBy("name").desc()
                .build();
        List<DocumentEntity> entities = entityManager.select(query);
        List<String> result = entities.stream().flatMap(e -> e.getDocuments().stream())
                .filter(d -> "name".equals(d.getName()))
                .map(d -> d.get(String.class))
                .collect(Collectors.toList());

        assertFalse(result.isEmpty());
        assertThat(result, contains("name3", "name", "name", "name"));
    }


    @Test
    public void shouldFindDocumentById() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name"), Document.of("_key", "person:id")));
        Document id = entity.find("_id").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(id.getName()).eq(id.get())
                .build();

        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldFindDocumentByKey() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name"), Document.of("_key", "person:id")));

        Document id = entity.find("_key").get();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(id.getName()).eq(id.get())
                .build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }
}
