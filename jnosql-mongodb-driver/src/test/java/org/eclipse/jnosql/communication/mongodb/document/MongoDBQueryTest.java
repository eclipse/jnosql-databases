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

import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static jakarta.nosql.document.DocumentDeleteQuery.delete;
import static jakarta.nosql.document.DocumentQuery.select;

public class MongoDBQueryTest {

    public static final String COLLECTION_NAME = "person";
    private static DocumentCollectionManager entityManager;

    @BeforeAll
    public static void setUp() throws IOException {
        entityManager = ManagerFactorySupplier.INSTANCE.get("database");
    }

    @BeforeEach
    public void beforeEach() {
        DocumentDeleteQuery.delete().from(COLLECTION_NAME).delete(entityManager);
    }

    @Test
    @DisplayName("The query should execute A or B")
    public void shouldQuery() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);

        DocumentQuery query = select()
                .from(COLLECTION_NAME)
                .where("name").eq("Otavio")
                .or("name").eq("Poliana").build();

        Stream<DocumentEntity> stream = entityManager.select(query);
        long count = stream.count();
        Assertions.assertEquals(1L, count);
        entityManager.delete(delete().from(COLLECTION_NAME).build());

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
