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
package org.eclipse.jnosql.communication.couchbase.document;


import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static jakarta.nosql.document.DocumentQuery.select;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DocumentQueryTest {

    public static final String COLLECTION_NAME = "person";
    private CouchbaseDocumentCollectionManager entityManager;
    private static CouchbaseDocumentConfiguration configuration;

    {
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.delete("person:id");
        keyValueEntityManager.delete("person:id2");
        keyValueEntityManager.delete("person:id3");
        keyValueEntityManager.delete("person:id4");
    }

    @BeforeAll
    public static void beforeClass() throws InterruptedException {
        configuration = CouchbaseDocumentTcConfiguration.getTcConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        CouchbaseDocumentCollectionManager entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);

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

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineStart() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));

        Document name = entity.find("name").get();

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get())
                .skip(1L)
                .build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineLimitAndStart() {

        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get())
                .skip(2L)
                .limit(2L)
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(1, entities.size());

    }


    @Test
    public void shouldSelectAll() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));


        DocumentQuery query = select().from(COLLECTION_NAME).build();
        Optional<Document> name = entity.find("name");
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertTrue(entities.size() >= 4);
    }


    @Test
    public void shouldFindDocumentByName() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id4"),
                Document.of("name", "name3"), Document.of("_key", "person:id4")));

        Document name = entity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
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

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
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
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
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

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

}
