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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Statement;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import rx.functions.Action1;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static rx.Observable.just;

public class CouchbaseDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {

    private static final Consumer<DocumentEntity> NOOP = d -> {
    };
    private static final Action1<Throwable> ERROR_SAVE = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase save method");
    private static final Action1<Throwable> ERROR_FIND = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase find method");
    private static final Action1<Throwable> ERROR_DELETE = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase delete method");
    private static final Action1<Throwable> ERROR_N1QLQUERY = a -> new ExecuteAsyncQueryException("On error when try to execute couchbase n1qlQuery method");

    private final CouchbaseDocumentCollectionManager manager;

    CouchbaseDocumentCollectionManagerAsync(CouchbaseDocumentCollectionManager manager) {
        this.manager = manager;
    }


    @Override
    public void save(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, NOOP);
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, ttl, NOOP);
    }

    @Override
    public void save(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(callBack, "callBack is required");
        just(entity)
                .map(manager::save)
                .subscribe(callBack::accept, ERROR_SAVE);
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(callBack, "callBack is required");
        just(entity)
                .map(e -> manager.save(e, ttl))
                .subscribe(callBack::accept, ERROR_SAVE);
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, callBack);
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
    public void find(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        just(query).map(manager::find).subscribe(callBack::accept, ERROR_FIND);
    }


    /**
     * Executes the n1qlquery with params and then result que result
     *
     * @param n1qlQuery the query
     * @param params    the params
     * @param callback  the callback
     * @throws NullPointerException       when either n1qlQuery or params are null
     * @throws ExecuteAsyncQueryException an async error
     */
    public void n1qlQuery(String n1qlQuery, JsonObject params, Consumer<List<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(n -> manager.n1qlQuery(n, params))
                .subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    /**
     * Executes the n1qlquery  with params and then result que result
     *
     * @param n1qlQuery the query
     * @param params    the params
     * @param callback  the callback
     * @throws NullPointerException       when either n1qlQuery or params are null
     * @throws ExecuteAsyncQueryException an async error
     */
    public void n1qlQuery(Statement n1qlQuery, JsonObject params, Consumer<List<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(n -> manager.n1qlQuery(n, params))
                .subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    /**
     * Executes the n1qlquery  plain query and then result que result
     *
     * @param n1qlQuery the query
     * @param callback  the callback
     * @throws NullPointerException       when either n1qlQuery or params are null
     * @throws ExecuteAsyncQueryException an async error
     */
    public void n1qlQuery(String n1qlQuery, Consumer<List<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(manager::n1qlQuery).subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    /**
     * Executes the n1qlquery  plain query and then result que result
     *
     * @param n1qlQuery the query
     * @param callback  the callback
     * @throws NullPointerException       when either n1qlQuery or params are null
     * @throws ExecuteAsyncQueryException an async error
     */
    public void n1qlQuery(Statement n1qlQuery, Consumer<List<DocumentEntity>> callback) throws NullPointerException, ExecuteAsyncQueryException {
        requireNonNull(callback, "callback is required");
        just(n1qlQuery).map(manager::n1qlQuery).subscribe(callback::accept, ERROR_N1QLQUERY);
    }

    @Override
    public void close() {
        manager.close();
    }
}
