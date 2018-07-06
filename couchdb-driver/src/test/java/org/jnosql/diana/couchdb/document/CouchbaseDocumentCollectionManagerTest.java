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
package org.jnosql.diana.couchdb.document;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.api.document.query.DocumentQueryBuilder;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.couchdb.CouchbaseUtil;
import org.jnosql.diana.couchdb.key.CouchbaseKeyValueConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.couchbase.client.java.query.dsl.Expression.x;
import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CouchbaseDocumentCollectionManagerTest {

    public static final String COLLECTION_NAME = "person";
    private CouchbaseDocumentCollectionManager entityManager;

    {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        CouhbaseDocumentCollectionManagerFactory managerFactory = configuration.get();
        entityManager = managerFactory.get(CouchbaseUtil.BUCKET_NAME);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.remove("person:id");
    }

    @Test
    public void shouldSave() {
        DocumentEntity entity = getEntity();
        DocumentEntity documentEntity = entityManager.insert(entity);
        assertEquals(entity, documentEntity);
    }

    @Test
    public void shouldSaveWithKey() {
        DocumentEntity entity = getEntity();
        entity.add("_key", "anyvalue");
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

        Document name = documentEntity.find("name").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(name.getName()).eq(name.get()).build();
        DocumentDeleteQuery deleteQuery = DocumentQueryBuilder.delete().from(COLLECTION_NAME)
                .where(name.getName()).eq(name.get()).build();
        entityManager.delete(deleteQuery);
        assertTrue(entityManager.select(query).isEmpty());
    }

    @Test
    public void shouldSaveSubDocument() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", Document.of("mobile", "1231231")));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Thread.sleep(5_00L);
        Document id = entitySaved.find("_id").get();



        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, contains(Document.of("mobile", "1231231")));
    }

    @Test
    public void shouldSaveSubDocument2() throws InterruptedException {
        DocumentEntity entity = getEntity();
        entity.add(Document.of("phones", asList(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231"))));
        DocumentEntity entitySaved = entityManager.insert(entity);
        Thread.sleep(1_00L);
        Document id = entitySaved.find("_id").get();
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.select(query).get(0);
        Document subDocument = entityFound.find("phones").get();
        List<Document> documents = subDocument.get(new TypeReference<List<Document>>() {
        });
        assertThat(documents, contains(Document.of("mobile", "1231231"), Document.of("mobile2", "1231231")));
    }

    @Test
    public void shouldSaveSetDocument() throws InterruptedException {
        Set<String> set = new HashSet<>();
        set.add("Acarajé");
        set.add("Munguzá");
        DocumentEntity entity = DocumentEntity.of(COLLECTION_NAME);
        entity.add(Document.of("_id", "id"));
        entity.add(Document.of("foods", set));
        entityManager.insert(entity);
        Document id = entity.find("_id").get();
        Thread.sleep(1_000L);
        DocumentQuery query = select().from(COLLECTION_NAME).where(id.getName()).eq(id.get()).build();
        DocumentEntity entityFound = entityManager.singleResult(query).get();
        Optional<Document> foods = entityFound.find("foods");
        Set<String> setFoods = foods.get().get(new TypeReference<Set<String>>() {
        });
        assertEquals(set, setFoods);
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
        DocumentQuery query = select().from("AppointmentBook").where(key.getName()).eq(key.get()).build();

        DocumentEntity documentEntity = entityManager.singleResult(query).get();
        assertNotNull(documentEntity);

        List<List<Document>> contacts = (List<List<Document>>) documentEntity.find("contacts").get().get();

        assertEquals(3, contacts.size());
        assertTrue(contacts.stream().allMatch(d -> d.size() == 3));
    }

    private DocumentEntity createSubdocumentList() {
        DocumentEntity entity = DocumentEntity.of("AppointmentBook");
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


    @Test
    public void shouldRunN1Ql() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        List<DocumentEntity> entities = entityManager.n1qlQuery("select * from jnosql");
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldRunN1QlParameters() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);
        JsonObject params = JsonObject.create().put("name", "Poliana");
        List<DocumentEntity> entities = entityManager.n1qlQuery("select * from jnosql where name = $name", params);
        assertFalse(entities.isEmpty());
        assertEquals(1, entities.size());
    }

    @Test
    public void shouldRunN1QlStatement() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);

        Statement statement = Select.select("*").from("jnosql").where(x("name").eq("\"Poliana\""));
        List<DocumentEntity> entities = entityManager.n1qlQuery(statement);
        assertFalse(entities.isEmpty());
        assertEquals(1, entities.size());
    }

    @Test
    public void shouldRunN1QlStatementParams() {
        DocumentEntity entity = getEntity();
        entityManager.insert(entity);

        Statement statement = Select.select("*").from("jnosql").where(x("name").eq("$name"));
        JsonObject params = JsonObject.create().put("name", "Poliana");
        List<DocumentEntity> entities = entityManager.n1qlQuery(statement, params);
        assertFalse(entities.isEmpty());
        assertEquals(1, entities.size());
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