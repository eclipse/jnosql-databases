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
package org.eclipse.jnosql.databases.couchbase.communication;


import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The default implementation of {@link CouchbaseDocumentManager}
 */
class DefaultCouchbaseDocumentManager implements CouchbaseDocumentManager {

    private static final Logger LOGGER = Logger.getLogger(DefaultCouchbaseDocumentManager.class.getName());

    private final Bucket bucket;
    private final String database;

    private final Cluster cluster;

    DefaultCouchbaseDocumentManager(Cluster cluster, String database) {
        this.bucket = cluster.bucket(database);
        this.database = database;
        this.cluster = cluster;
    }

    @Override
    public String name() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        return waitBucketBeReadyAndGet(() -> {
            entity.add(EntityConverter.COLLECTION_FIELD, entity.name());
            JsonObject json = EntityConverter.convert(entity);
            Document id = entity.find(EntityConverter.ID_FIELD)
                    .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));
            Collection collection = bucket.collection(entity.name());
            collection.insert(id.get(String.class), json);
            return entity;
        });
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        return waitBucketBeReadyAndGet(() -> {
            JsonObject json = EntityConverter.convert(entity);
            Document id = entity.find(EntityConverter.ID_FIELD)
                    .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));
            Collection collection = bucket.collection(entity.name());
            collection.insert(id.get(String.class), json, InsertOptions.insertOptions().expiry(ttl));
            return entity;
        });
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert).collect(Collectors.toList());
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> insert(e, ttl)).collect(Collectors.toList());
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");
        return waitBucketBeReadyAndGet(() -> {
            entity.add(EntityConverter.COLLECTION_FIELD, entity.name());
            JsonObject json = EntityConverter.convert(entity);
            Document id = entity.find(EntityConverter.ID_FIELD)
                    .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));
            Collection collection = bucket.collection(entity.name());
            collection.upsert(id.get(String.class), json);
            return entity;
        });
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update).collect(Collectors.toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        waitBucketBeReadyAndDo(() -> {
            Objects.requireNonNull(query, "query is required");
            Collection collection = bucket.collection(query.name());
            DocumentQuery delete = DeleteQueryWrapper.of(query);
            Stream<DocumentEntity> entities = select(delete);
            entities.flatMap(d -> d.find(EntityConverter.ID_FIELD).stream())
                    .filter(Objects::nonNull)
                    .map(d -> d.get(String.class))
                    .forEach(collection::remove);
        });
    }

    private void waitBucketBeReadyAndDo(Runnable runnable) {
        bucket.waitUntilReady(bucket.environment().timeoutConfig().kvDurableTimeout());
        runnable.run();
    }


    private <T> T waitBucketBeReadyAndGet(Supplier<T> supplier) {
        bucket.waitUntilReady(bucket.environment().timeoutConfig().kvDurableTimeout());
        return supplier.get();
    }

    @Override
    public Stream<DocumentEntity> select(final DocumentQuery query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        return waitBucketBeReadyAndGet(() -> {
            N1QLQuery n1QLQuery = N1QLBuilder.of(query, database, bucket.defaultScope().name()).get();
            List<JsonObject> jsons = new ArrayList<>();
            if (n1QLQuery.hasIds()) {
                Collection collection = bucket.collection(query.name());
                for (String id : n1QLQuery.getIds()) {
                    try {
                        GetResult result = collection.get(id);
                        jsons.add(result.contentAsObject());
                    } catch (DocumentNotFoundException exp) {
                        LOGGER.log(Level.FINEST, "The id was not found: " + id);
                    }
                }
            }

            if (!n1QLQuery.hasOnlyIds()) {
                QueryResult result;
                if (n1QLQuery.hasParameter()) {
                    result = cluster.query(n1QLQuery.getQuery());
                } else {
                    result = cluster.query(n1QLQuery.getQuery(), QueryOptions
                            .queryOptions().parameters(n1QLQuery.getParams()));
                }
                jsons.addAll(result.rowsAsObject());
            }
            return EntityConverter.convert(jsons, database);
        });
    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");
        return waitBucketBeReadyAndGet(() -> {
            DocumentQuery countQuery = DocumentQuery
                    .select("COUNT(*)").from(documentCollection).build();
            N1QLQuery n1QLQuery = N1QLBuilder
                    .of(countQuery, database, bucket.defaultScope().name()).get();
            QueryResult query = cluster.query(n1QLQuery.getQuery());
            List<JsonObject> result = query.rowsAsObject();
            var count = result.stream().findFirst()
                    .map(data -> data.getNumber("$1"))
                    .orElse(0L);
            return count.longValue();
        });
    }

    @Override
    public Stream<DocumentEntity> n1qlQuery(final String n1ql, final JsonObject params) throws NullPointerException {
        requireNonNull(n1ql, "n1qlQuery is required");
        requireNonNull(params, "params is required");
        return waitBucketBeReadyAndGet(() -> {
            QueryResult query = cluster.query(n1ql, QueryOptions
                    .queryOptions().parameters(params));
            return EntityConverter.convert(query.rowsAsObject(), database);
        });
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1ql) throws NullPointerException {
        requireNonNull(n1ql, "n1qlQuery is required");
        return waitBucketBeReadyAndGet(() -> {
            QueryResult query = cluster.query(n1ql);
            return EntityConverter.convert(query.rowsAsObject(), database);
        });
    }


    @Override
    public void close() {
    }
}
