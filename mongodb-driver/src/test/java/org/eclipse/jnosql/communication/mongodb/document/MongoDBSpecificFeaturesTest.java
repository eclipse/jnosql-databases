/*
 *  Copyright (c) 2022 Otávio Santana and others
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
package org.eclipse.jnosql.communication.mongodb.document;

import com.mongodb.client.model.Filters;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;
import static jakarta.nosql.document.DocumentQuery.select;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class MongoDBSpecificFeaturesTest {

    public static final String COLLECTION_NAME = "person";
    private static MongoDBDocumentCollectionManager entityManager;

    @BeforeAll
    public static void setUp() throws IOException {
        entityManager = ManagerFactorySupplier.INSTANCE.get("database");
    }

    @BeforeEach
    public void beforeEach() {
        DocumentDeleteQuery.delete().from(COLLECTION_NAME).delete(entityManager);
    }

    @Test
    public void shouldReturnErrorOnSelectWhenThereIsNullParameter(){
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.select(null, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.select(COLLECTION_NAME, null));

        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.select(null,  eq("name", "Poliana")));
    }

    @Test
    public void shouldFindDocument() {
        DocumentEntity entity = entityManager.insert(getEntity());

        List<DocumentEntity> entities = entityManager.select(COLLECTION_NAME,
                eq("name", "Poliana")).collect(Collectors.toList());
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldReturnErrorOnDeleteWhenThereIsNullParameter(){
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.delete(null, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.delete(COLLECTION_NAME, null));

        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.delete(null,  eq("name", "Poliana")));
    }

    @Test
    public void shouldDelete() {
        entityManager.insert(getEntity());

        long result = entityManager.delete(COLLECTION_NAME,
                eq("name", "Poliana"));

        Assertions.assertEquals(1L, result);
        List<DocumentEntity> entities = entityManager.select(COLLECTION_NAME,
                eq("name", "Poliana")).collect(Collectors.toList());
        Assertions.assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldReturnErrorOnAggregateWhenThereIsNullParameter(){
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.aggregate(null, null));
        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.aggregate(COLLECTION_NAME, null));

        Assertions.assertThrows(NullPointerException.class,
                () -> entityManager.aggregate(null,
                        Collections.singletonList(eq("name", "Poliana"))));
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

}
