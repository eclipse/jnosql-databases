/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.arangodb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteCondition;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static org.jnosql.diana.arangodb.document.DocumentConfigurationUtils.getConfiguration;


public class ArangoDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";
    private DocumentCollectionManagerAsync entityManagerAsync;
    private DocumentCollectionManager entityManager;
    private Random random;
    private String KEY_NAME = "_key";

    @Before
    public void setUp() {
        random = new Random();
        entityManagerAsync = getConfiguration().getAsync("database");
        entityManager = getConfiguration().get("database");
    }


    @Test
    public void shouldSaveAsync() {
        DocumentEntity entity = getEntity();
        entityManagerAsync.save(entity);

    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.save(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() {
        DocumentEntity documentEntity = entityManager.save(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find(KEY_NAME);
        query.and(DocumentCondition.eq(id.get()));
        entityManagerAsync.delete(DocumentDeleteCondition.of(query.getCollection(), query.getCondition()));
    }


    private DocumentEntity getEntity() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Poliana");
        map.put("city", "Salvador");
        entity.add(Document.of(KEY_NAME, random.nextLong()));
        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }
}