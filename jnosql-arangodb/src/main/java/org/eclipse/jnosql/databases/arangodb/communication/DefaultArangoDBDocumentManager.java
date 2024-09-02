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
import org.eclipse.jnosql.communication.semistructured.CommunicationEntity;
import org.eclipse.jnosql.communication.semistructured.DeleteQuery;
import org.eclipse.jnosql.communication.semistructured.Element;
import org.eclipse.jnosql.communication.semistructured.SelectQuery;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.arangodb.internal.ArangoErrors.ERROR_ARANGO_DATA_SOURCE_NOT_FOUND;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

class DefaultArangoDBDocumentManager implements ArangoDBDocumentManager {

    private static final Logger LOGGER = Logger.getLogger(DefaultArangoDBDocumentManager.class.getName());

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
    public CommunicationEntity insert(CommunicationEntity entity)  {
        Objects.requireNonNull(entity, "entity is required");
        String collectionName = entity.name();
        checkCollection(collectionName);
        BaseDocument baseDocument = ArangoDBUtil.getBaseDocument(entity);
        DocumentCreateEntity<Void> arandoDocument = arangoDB.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        updateEntity(entity, arandoDocument.getKey(), arandoDocument.getId(), arandoDocument.getRev());
        return entity;
    }

    @Override
    public CommunicationEntity update(CommunicationEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
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
    public Iterable<CommunicationEntity> update(Iterable<CommunicationEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .collect(Collectors.toList());
    }

    @Override
    public void delete(DeleteQuery query) {
        requireNonNull(query, "query is required");
        try {

            if (query.condition().isEmpty()) {
                AQLQueryResult delete = QueryAQLConverter.delete(query);
                arangoDB.db(database).query(delete.query(), BaseDocument.class);
                return;
            }

            AQLQueryResult delete = QueryAQLConverter.delete(query);
            arangoDB.db(database).query(delete.query(), BaseDocument.class, delete.values(),
                    null);
        } catch (com.arangodb.ArangoDBException exception) {
            if (ERROR_ARANGO_DATA_SOURCE_NOT_FOUND.equals(exception.getErrorNum())) {
                LOGGER.log(Level.FINEST, exception, () -> "An error to run query, that is related to delete " +
                        "a document collection that does not exist");
            } else {
                throw exception;
            }
        }
    }

    @Override
    public Stream<CommunicationEntity> select(SelectQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        AQLQueryResult result = QueryAQLConverter.select(query);
        LOGGER.finest("Executing AQL: " + result.query());
        ArangoCursor<BaseDocument> documents = arangoDB.db(database).query(result.query(),
                BaseDocument.class,
                result.values(), null);

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
    public Stream<CommunicationEntity> aql(String query, Map<String, Object> params) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(params, "values is required");
        ArangoCursor<BaseDocument> result = arangoDB.db(database).query(query,BaseDocument.class, params, null);
        return StreamSupport.stream(result.spliterator(), false)
                .map(ArangoDBUtil::toEntity);

    }

    @Override
    public <T> Stream<T> aql(String query, Map<String, Object> params, Class<T> type) {
        requireNonNull(query, "query is required");
        requireNonNull(params, "values is required");
        requireNonNull(type, "typeClass is required");
        ArangoCursor<T> result = arangoDB.db(database).query(query, type, params, null);
        return StreamSupport.stream(result.spliterator(), false);
    }

    @Override
    public <T> Stream<T> aql(String query, Class<T> type) {
        requireNonNull(query, "query is required");
        requireNonNull(type, "typeClass is required");
        ArangoCursor<T> result = arangoDB.db(database).query(query, type, emptyMap(), null);
        return StreamSupport.stream(result.spliterator(), false);
    }


    @Override
    public void close() {
        arangoDB.shutdown();
    }


    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }

    @Override
    public CommunicationEntity insert(CommunicationEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public Iterable<CommunicationEntity> insert(Iterable<CommunicationEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<CommunicationEntity> insert(Iterable<CommunicationEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.insert(e, ttl))
                .collect(Collectors.toList());
    }

    private void updateEntity(CommunicationEntity entity, String key, String id, String rev) {
        entity.add(Element.of(KEY, key));
        entity.add(Element.of(ID, id));
        entity.add(Element.of(REV, rev));
    }

    private static void feedKey(CommunicationEntity entity, String id) {
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
