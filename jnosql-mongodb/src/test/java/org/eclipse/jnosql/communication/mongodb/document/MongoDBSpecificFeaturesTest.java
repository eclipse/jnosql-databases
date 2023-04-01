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
package org.eclipse.jnosql.communication.mongodb.document;

import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.bson.BsonValue;
import org.bson.conversions.Bson;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.mongodb.client.model.Filters.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.INTEGRATION_MATCHES;
import static org.junit.jupiter.api.Assertions.assertFalse;

@EnabledIfSystemProperty(named = INTEGRATION, matches = INTEGRATION_MATCHES)
public class MongoDBSpecificFeaturesTest {

    public static final String COLLECTION_NAME = "person";
    private static MongoDBDocumentManager entityManager;

    @BeforeAll
    public static void setUp() throws IOException {
        entityManager = DocumentDatabase.INSTANCE.get("database");
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
        assertThat(entities).contains(entity);
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

    @Test
    public void shouldAggregate() {
        List<Bson> predicates = Arrays.asList(
                Aggregates.match(eq("name", "Poliana")),
                Aggregates.group("$stars", Accumulators.sum("count", 1))
        );
        entityManager.insert(getEntity());
        Stream<Map<String, BsonValue>> aggregate = entityManager.aggregate(COLLECTION_NAME, predicates);
        Assertions.assertNotNull(aggregate);
        Map<String, BsonValue> result = aggregate.findFirst()
                .orElseThrow(() -> new IllegalStateException("There is an issue with the aggregate test result"));

        Assertions.assertNotNull(result);
        Assertions.assertFalse(result.isEmpty());
        BsonValue count = result.get("count");
        Assertions.assertEquals(1L, count.asNumber().longValue());

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
