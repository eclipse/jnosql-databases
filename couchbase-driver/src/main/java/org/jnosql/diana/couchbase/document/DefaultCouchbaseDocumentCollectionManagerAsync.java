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


import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Statement;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import rx.functions.Action1;

import java.time.Duration;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static rx.Observable.just;

class DefaultCouchbaseDocumentCollectionManagerAsync implements CouchbaseDocumentCollectionManagerAsync {

    private static final Consumer<DocumentEntity> NOOP = d -> {
    };
    private static final Action1<Throwable> ERROR_SAVE = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase save method");
    private static final Action1<Throwable> ERROR_FIND = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase find method");
    private static final Action1<Throwable> ERROR_DELETE = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase delete method");
    private static final Action1<Throwable> ERROR_N1QLQUERY = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase n1qlQuery method");

    private final CouchbaseDocumentCollectionManager manager;

    DefaultCouchbaseDocumentCollectionManagerAsync(CouchbaseDocumentCollectionManager manager) {
        this.manager = manager;
    }


    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, NOOP);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, ttl, NOOP);
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
        entities.forEach(e -> this.insert(e, ttl));
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(callBack, "callBack is required");
        just(entity)
                .map(manager::insert)
                .subscribe(callBack::accept, ERROR_SAVE);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(callBack, "callBack is required");
        just(entity)
                .map(e -> manager.insert(e, ttl))
                .subscribe(callBack::accept, ERROR_SAVE);
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity);
    }

    @Override
    public void update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::update);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, callBack);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, v -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        just(query).map(q -> {
            manager.delete(q);
            return true;
        }).subscribe(a -> callBack.accept(null), ERROR_DELETE);
    }

    @Override
    public void select(DocumentQuery query, Consumer<Stream<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        just(query).map(manager::select).subscribe(callBack::accept, ERROR_FIND);
    }

    @Override
    public void count(String documentCollection, Consumer<Long> callback) {
        throw new UnsupportedOperationException("Couchbase does not support count method by document collection");
    }


    @Override
    public void n1qlQuery(String n1qlQuery, JsonObject params, Consumer<Stream<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(n -> manager.n1qlQuery(n, params))
                .subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    @Override
    public void n1qlQuery(Statement n1qlQuery, JsonObject params, Consumer<Stream<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(n -> manager.n1qlQuery(n, params))
                .subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    @Override
    public void n1qlQuery(String n1qlQuery, Consumer<Stream<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(manager::n1qlQuery).subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    @Override
    public void n1qlQuery(Statement n1qlQuery, Consumer<Stream<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(manager::n1qlQuery).subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    @Override
    public void close() {
        manager.close();
    }
}
