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

import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.runAsync;
import static java.util.concurrent.CompletableFuture.supplyAsync;

final class DefaultCouchDBDocumentCollectionManagerAsync implements CouchDBDocumentCollectionManagerAsync {

    private final CouchDBDocumentCollectionManager manager;

    DefaultCouchDBDocumentCollectionManagerAsync(CouchDBDocumentCollectionManager manager) {
        this.manager = manager;
    }


    @Override
    public void insert(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");
        supplyAsync(() -> manager.insert(entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
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
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.insert(entity));
        async.thenAccept(callBack::accept);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("CouchDB does not support TTL operation");
    }

    @Override
    public void update(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.update(entity));
    }

    @Override
    public void update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::update);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        CompletableFuture<DocumentEntity> async = supplyAsync(() -> manager.update(entity));
        async.thenAccept(callBack::accept);
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        requireNonNull(query, "query is required");
        runAsync(() -> manager.delete(query));
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        CompletableFuture<Void> async = runAsync(() -> manager.delete(query));
        async.thenAccept(callBack::accept);

    }

    @Override
    public void select(DocumentQuery query, Consumer<Stream<DocumentEntity>> callBack) {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        CompletableFuture<Stream<DocumentEntity>> async = supplyAsync(() -> manager.select(query));
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
        requireNonNull(callback, "callback is required");
        CompletableFuture<Long> async = supplyAsync(() -> manager.count());
        async.thenAccept(callback::accept);
    }
}
