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


import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE.ASYNCHRONOUS;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.VERSION_FIELD;
import static org.jnosql.diana.orientdb.document.QueryOSQLFactory.toAsync;

class DefaultOrientDBDocumentCollectionManagerAsync implements OrientDBDocumentCollectionManagerAsync {

    private static final Consumer<DocumentEntity> NOOPS = d -> {
    };
    private static final int FIRST_VERSION = 1;


    private final ODatabasePool pool;

    DefaultOrientDBDocumentCollectionManagerAsync(ODatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("There is support to ttl on OrientDB");
    }

    @Override
    public void insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::insert);
    }

    @Override
    public void insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        entities.forEach(e -> insert(e, ttl));
    }

    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, NOOPS);
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "Entity is required");
        requireNonNull(callBack, "Callback is required");

        ODatabaseSession tx = pool.acquire();
        ODocument document = new ODocument(entity.getName());
        Map<String, Object> entityValues = entity.toMap();
        entityValues.keySet().stream().forEach(k -> document.field(k, entityValues.get(k)));
        ORecordCallback<Number> createCallBack = (rid, clusterPosition) -> {
            entity.add(Document.of(RID_FIELD, rid.toString()));
            entity.add(Document.of(VERSION_FIELD, FIRST_VERSION));
            callBack.accept(entity);
        };
        ORecordCallback<Integer> updateCallback = (rid, version) -> {
            entity.add(Document.of(RID_FIELD, rid.toString()));
            entity.add(Document.of(VERSION_FIELD, version));
            callBack.accept(entity);
        };
        tx.save(document, null, ASYNCHRONOUS, false, createCallBack, updateCallback);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("There is no support to ttl on OrientDB");
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        insert(entity);
    }

    @Override
    public void update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::update);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        insert(entity, callBack);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(query, "query is required");
        delete(query, v -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        ODatabaseSession tx = pool.acquire();
        DocumentQuery selectQuery = new OrientDBDocumentQuery(query);

        QueryOSQLFactory.QueryResultAsync orientQuery = toAsync(selectQuery, l -> {
            l.forEach(ORecordAbstract::delete);
            callBack.accept(null);
        });
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        ODatabaseSession tx = pool.acquire();
        QueryOSQLFactory.QueryResultAsync orientQuery = toAsync(query, l -> callBack.accept(l.stream()
                .map(OrientDBConverter::convert)
                .collect(toList())));
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    @Override
    public void count(String documentCollection, Consumer<Long> callback) {
        requireNonNull(documentCollection, "query is required");
        requireNonNull(callback, "callBack is required");

        ODatabaseSession tx = pool.acquire();

        String query = "select count(*) from ".concat(documentCollection);
        Consumer<List<ODocument>> orientCallback = l ->
                callback.accept(l.stream()
                        .findFirst()
                        .map(e -> e.field("count"))
                        .map(n -> Number.class.cast(n).longValue())
                        .orElse(0L));
        QueryOSQLFactory.QueryResultAsync orientQuery = toAsync(query, orientCallback);
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    @Override
    public void sql(String query, Consumer<List<DocumentEntity>> callBack, Object... params) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        ODatabaseSession tx = pool.acquire();
        QueryOSQLFactory.QueryResultAsync orientQuery = toAsync(query, l -> callBack.accept(l.stream()
                .map(OrientDBConverter::convert)
                .collect(toList())), params);
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    @Override
    public void sql(String query, Consumer<List<DocumentEntity>> callBack, Map<String, Object> params) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        requireNonNull(params, "params is required");

        ODatabaseSession tx = pool.acquire();
        QueryOSQLFactory.QueryResultAsync orientQuery = toAsync(query, l -> callBack.accept(l.stream()
                .map(OrientDBConverter::convert)
                .collect(toList())), params);
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }


    @Override
    public void close() {
        pool.close();
    }
}
