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

import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.queries.MatchQuery;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.couchbase.key.CouchbaseKeyValueConfiguration;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class CouchbaseDocumentCollectionManagerTest {

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
    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }


    @Test
    public void shouldUpdateSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        Document newField = Documents.of("newField", "10");
        entity.add(newField);
        DocumentEntity updated = entityManager.update(entity);
        assertEquals(newField, updated.find("newField").get());
    }

    @Test
    public void shouldRemoveEntityByName() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> name = documentEntity.find("name");
        query.and(DocumentCondition.eq(name.get()));
        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldRemoveEntityById() {
        DocumentEntity documentEntity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = documentEntity.find("_id");
        query.and(DocumentCondition.eq(id.get()));
        entityManager.delete(DocumentDeleteQuery.of(query.getCollection(), query.getCondition().get()));
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldFindDocumentByName() {
        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> name = entity.find("name");
        query.and(DocumentCondition.eq(name.get()));
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldShouldDefineLimit() {
        DocumentEntity entity = DocumentEntity.of("person", asList(Document.of("_id", "id")
                , Document.of("name", "name")));
        DocumentEntity entity2 = DocumentEntity.of("person", asList(Document.of("_id", "id2")
                , Document.of("name", "name")));
        DocumentEntity entity3 = DocumentEntity.of("person", asList(Document.of("_id", "id3")
                , Document.of("name", "name")));

        entityManager.insert(Arrays.asList(entity, entity2, entity3));

        Optional<Document> name = entity.find("name");
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(name.get()));
        query.withMaxResults(2L);
        List<DocumentEntity> entities = entityManager.select(query);
        assertEquals(2, entities.size());

        entityManager.delete(query.toDeleteQuery());
        assertTrue(entityManager.select(query).isEmpty());

    }

    @Test
    public void shouldSelectAll() {
        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> name = entity.find("name");
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldFindDocumentById() {
        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("_id");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }

    @Test
    public void shouldFindDocumentByKey() {
        DocumentEntity entity = entityManager.insert(getEntity());
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        Optional<Document> id = entity.find("_key");
        query.and(DocumentCondition.eq(id.get()));
        List<DocumentEntity> entities = entityManager.select(query);
        assertFalse(entities.isEmpty());
        assertThat(entities, contains(entity));
    }


    @Test
    public void shouldSaveSubDocument() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, contains(Document.of("mobile", "1231231")));
    }

    @Test
    public void shouldSaveSubDocument2() {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, contains(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231")));
    }

    @Test
    public void shouldSaveSetDocument() {
        Set<String> set = new HashSet<>();
        set.add("Acarajé");
        set.add("Munguzá");
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        entity.add(Document.of("_id", "id"));
        entity.add(Document.of("foods", set));
        entityManager.insert(entity);
        Document id = entity.find("_id").get();
        DocumentQuery query = DocumentQuery.of(COLLECTION_NAME);
        query.and(DocumentCondition.eq(id));
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Optional<Document> foods = entityFound.find("foods");
        Set<String> setFoods = foods.get().get(new TypeReference<Set<String>>() {
        });
        assertEquals(set, setFoods);
    }

    @Test
    @Ignore
    public void shouldSearchElement() {
        DocumentEntity entity = getEntity();
        entity.add("description", "Founded by the Portuguese in 1549 as the first capital of Brazil, Salvador is" +
                " one of the oldest colonial cities in the Americas.");
        entityManager.insert(entity);
        MatchQuery fts = SearchQuery.match("Salvador");
        SearchQuery query = new SearchQuery("search-index-diana", fts).fields("city");
        List<DocumentEntity> entities = entityManager.search(query);
        System.out.println(entities);
    }

    private DocumentEntity getEntity() {
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        Map<String, Object> map = new HashMap<>();
        map.put("name", "Poliana");
        map.put("city", "Salvador");
        map.put("_id", "id");

        List<Document> documents = Documents.of(map);
        documents.forEach(entity::add);
        return entity;
    }

}