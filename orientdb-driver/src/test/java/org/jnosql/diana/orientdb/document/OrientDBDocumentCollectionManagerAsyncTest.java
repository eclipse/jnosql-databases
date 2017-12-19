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
package org.jnosql.diana.orientdb.document;

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.logging.Logger;

import static java.util.logging.Level.FINEST;
import static org.jnosql.diana.api.document.DocumentCondition.eq;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.delete;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.jnosql.diana.orientdb.document.DocumentConfigurationUtils.getAsync;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class OrientDBDocumentCollectionManagerAsyncTest {

    public static final String COLLECTION_NAME = "person";

    private DocumentCollectionManagerAsync entityManagerAsync;

    private DocumentCollectionManager entityManager;

    private static final Logger LOGGER = Logger.getLogger(OrientDBDocumentCollectionManagerTest.class.getName());

    @Before
    public void setUp() {
        entityManagerAsync = getAsync().getAsync("database");
        entityManager = DocumentConfigurationUtils.get().get("database");
        DocumentEntity documentEntity = getEntity();
        Document id = documentEntity.find("name").get();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        try {
            entityManagerAsync.delete(deleteQuery);
        } catch (Exception e) {
            LOGGER.log(FINEST, "error on OrientDB setup", e);
        }
    }


    @Test
    public void shouldSaveAsync() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entityManagerAsync.insert(entity);

        Thread.sleep(1_000L);
        Document id = entity.find("name").get();

        DocumentQuery query =  select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());

    }

    @Test
    public void shouldUpdateAsync() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        entityManagerAsync.update(entity);
    }

    @Test
    public void shouldRemoveEntityAsync() throws InterruptedException {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        Document id = documentEntity.find("name").get();

        DocumentQuery query =  select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentDeleteQuery deleteQuery = delete().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();

        entityManagerAsync.delete(deleteQuery);
        Thread.sleep(1_000L);
        assertTrue(entityManager.select(query).isEmpty());

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