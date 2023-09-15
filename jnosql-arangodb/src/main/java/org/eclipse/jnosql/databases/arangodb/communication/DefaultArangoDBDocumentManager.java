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
package org.eclipse.jnosql.databases.arangodb.communication;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

class DefaultArangoDBDocumentManager implements ArangoDBDocumentManager {


    public static final String KEY = "_key";
    public static final String ID = "_id";
    public static final String REV = "_rev";

    private final String database;

    private final ArangoDB arangoDB;

    DefaultArangoDBDocumentManager(String database, ArangoDB arangoDB) {
        this.database = database;
        this.arangoDB = arangoDB;
    }

    @Override
    public String name() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        String collectionName = entity.name();
        checkCollection(collectionName);
        BaseDocument baseDocument = ArangoDBUtil.getBaseDocument(entity);
        DocumentCreateEntity<Void> arangoDocument = arangoDB.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        updateEntity(entity, arangoDocument.getKey(), arangoDocument.getId(), arangoDocument.getRev());
        return entity;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        String collectionName = entity.name();
        checkCollection(collectionName);
        String id = entity.find(ID, String.class)
                .orElseThrow(() -> new IllegalArgumentException("The document does not provide" +
                        " the _id column"));
        feedKey(entity, id);
        BaseDocument baseDocument = ArangoDBUtil.getBaseDocument(entity);
        DocumentUpdateEntity<Void> arandoDocument = arangoDB.db(database)
                .collection(collectionName).updateDocument(baseDocument.getKey(), baseDocument);
        updateEntity(entity, arandoDocument.getKey(), arandoDocument.getId(), arandoDocument.getRev());
        return entity;
    }


    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .collect(Collectors.toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        requireNonNull(query, "query is required");
        if (checkCondition(query.condition())) {
            return;
        }

        AQLQueryResult delete = QueryAQLConverter.delete(query);
        arangoDB.db(database).query(delete.getQuery(), BaseDocument.class, delete.getValues(),
                null);
    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");

        AQLQueryResult result = QueryAQLConverter.select(query);
        ArangoCursor<BaseDocument> documents = arangoDB.db(database).query(result.getQuery(),
                BaseDocument.class,
                result.getValues(), null);

        return StreamSupport.stream(documents.spliterator(), false)
                .map(ArangoDBUtil::toEntity);
    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "document collection is required");
        String aql = "RETURN LENGTH(" + documentCollection + ")";
        ArangoCursor<Object> query = arangoDB.db(database).query(aql, Object.class, emptyMap(), null);
        return StreamSupport.stream(query.spliterator(), false).findFirst().map(Number.class::cast)
                .map(Number::longValue).orElse(0L);
    }


    @Override
    public Stream<DocumentEntity> aql(String query, Map<String, Object> values) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");
        ArangoCursor<BaseDocument> result = arangoDB.db(database).query(query,BaseDocument.class, values, null);
        return StreamSupport.stream(result.spliterator(), false)
                .map(ArangoDBUtil::toEntity);

    }

    @Override
    public <T> Stream<T> aql(String query, Map<String, Object> values, Class<T> typeClass) {
        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");
        requireNonNull(typeClass, "typeClass is required");
        ArangoCursor<T> result = arangoDB.db(database).query(query, typeClass, values, null);
        return StreamSupport.stream(result.spliterator(), false);
    }

    @Override
    public <T> Stream<T> aql(String query, Class<T> typeClass) {
        requireNonNull(query, "query is required");
        requireNonNull(typeClass, "typeClass is required");
        ArangoCursor<T> result = arangoDB.db(database).query(query,typeClass, emptyMap(), null);
        return StreamSupport.stream(result.spliterator(), false);
    }


    @Override
    public void close() {
        arangoDB.shutdown();
    }


    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }

    private boolean checkCondition(Optional<DocumentCondition> query) {
        return !query.isPresent();
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.insert(e, ttl))
                .collect(Collectors.toList());
    }

    private void updateEntity(DocumentEntity entity, String key, String id, String rev) {
        entity.add(Document.of(KEY, key));
        entity.add(Document.of(ID, id));
        entity.add(Document.of(REV, rev));
    }

    private static void feedKey(DocumentEntity entity, String id) {
        if (entity.find(KEY).isEmpty()) {
            String[] values = id.split("/");
            if (values.length == 2) {
                entity.add(KEY, values[1]);
            } else {
                entity.add(KEY, values[0]);
            }
        }
    }

    ArangoDB getArangoDB() {
        return arangoDB;
    }

}
