/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

final class DefaultCouchDBDocumentCollectionManagerAsync implements CouchDBDocumentCollectionManagerAsync {

    private CouchDBDocumentCollectionManager manager;

    @Override
    public void insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        supplyAsync(() -> manager.insert(entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(callBack, "callBack is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.insert(entity));
        async.thenAccept(callBack::accept);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public void update(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.update(entity));
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(callBack, "callBack is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.update(entity));
        async.thenAccept(callBack::accept);
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        runAsync(() -> manager.delete(query));
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBack, "callBack is required");
        CompletableFuture<Void> async = runAsync(() -> manager.delete(query));
        async.thenAccept(callBack::accept);

    }

    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBack, "callBack is required");
        CompletableFuture<List<DocumentEntity>> async = supplyAsync(() -> manager.select(query));
        async.thenAccept(callBack::accept);
    }

    @Override
    public void count(String documentCollection, Consumer<Long> callback) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public void close() {
        manager.close();
    }

    @Override
    public void count(Consumer<Long> callback) {
        Objects.requireNonNull(callback, "callback is required");
        CompletableFuture<Long> async = supplyAsync(() -> manager.count());
        async.thenAccept(callback::accept);
    }
}
