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
package org.eclipse.jnosql.databases.couchbase.communication;


import com.couchbase.client.core.env.TimeoutConfig;
import com.couchbase.client.core.error.DocumentExistsException;
import com.couchbase.client.core.error.DocumentNotFoundException;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.keyvalue.BucketManager;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
public class DocumentQueryTest {

    public static final String COLLECTION_NAME = "person";
    private static CouchbaseDocumentManager entityManager;
    private static BucketManager keyValueEntityManager;

    static {
        var settings = Database.INSTANCE.getSettings();
        var configuration = Database.INSTANCE.getDocumentConfiguration();
        CouchbaseDocumentManagerFactory managerFactory = configuration.apply(settings);
        entityManager = managerFactory.apply(CouchbaseUtil.BUCKET_NAME);
        BucketManagerFactory keyValueEntityManagerFactory =
                Database.INSTANCE.getKeyValueConfiguration().apply(settings);
        keyValueEntityManager = keyValueEntityManagerFactory.apply(CouchbaseUtil.BUCKET_NAME);
    }

    @AfterEach
    public void afterEach() {
        List.of("id", "id2", "id3", "id4")
                .forEach(key -> ignoreException(() -> keyValueEntityManager.delete(key)));

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);
    }


    @BeforeEach
    public void beforeEach() {

        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));
        DocumentEntity entity2 = DocumentEntity.of("person", asList(Document.of("_id", "id2")
                , Document.of("name", "name")));
        DocumentEntity entity3 = DocumentEntity.of("person", asList(Document.of("_id", "id3")
                , Document.of("name", "name")));
        DocumentEntity entity4 = DocumentEntity.of("person", asList(Document.of("_id", "id4")
                , Document.of("name", "name3")));

        entityManager.update(Arrays.asList(entity, entity2, entity3, entity4));

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);
    }

    private void ignoreException(Runnable runnable) {
        try {
            runnable.run();
        } catch (DocumentNotFoundException
                 | DocumentExistsException ex) {
            //IGNORED
        }
    }

    @Test
    public void shouldShouldDefineLimit() {

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("name")
                .limit(2L)
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineStart() {
        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("name")
                .skip(1L)
                .build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());

    }

    @Test
    public void shouldShouldDefineLimitAndStart() {

        List<DocumentEntity> entities = entityManager.select(select().from(COLLECTION_NAME).build()).collect(Collectors.toList());
        assertEquals(4, entities.size());

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name").eq("name")
                .skip(1L)
                .limit(2L)
                .build();

        entities = entityManager.select(query).collect(Collectors.toList());
        assertEquals(2, entities.size());

    }


    @Test
    public void shouldSelectAll() {
        DocumentQuery query = select().from(COLLECTION_NAME).build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities).hasSize(4);
    }


    @Test
    public void shouldFindDocumentByName() {

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("name")
                .eq("name")
                .build();
        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldFindDocumentByNameSortAsc() {

        DocumentQuery query = select().from(COLLECTION_NAME)
                .orderBy("name").asc()
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        List<String> result = entities.stream()
                .flatMap(e -> e.documents().stream())
                .filter(d -> "name".equals(d.name()))
                .map(d -> d.get(String.class))
                .collect(Collectors.toList());

        assertFalse(result.isEmpty());
        assertThat(result).contains("name", "name", "name", "name3");
    }

    @Test
    public void shouldFindDocumentByNameSortDesc() {

        DocumentQuery query = select().from(COLLECTION_NAME)
                .orderBy("name").desc()
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());

        List<String> result = entities.stream().flatMap(e -> e.documents().stream())
                .filter(d -> "name".equals(d.name()))
                .map(d -> d.get(String.class))
                .collect(Collectors.toList());

        assertFalse(result.isEmpty());
        assertThat(result).contains("name3", "name", "name", "name");
    }

    @Test
    public void shouldFindDocumentById() {

        DocumentQuery query = select().from(COLLECTION_NAME)
                .where("_id").eq("id")
                .build();

        List<DocumentEntity> entities = entityManager.select(query).collect(Collectors.toList());
        assertFalse(entities.isEmpty());

    }

}
