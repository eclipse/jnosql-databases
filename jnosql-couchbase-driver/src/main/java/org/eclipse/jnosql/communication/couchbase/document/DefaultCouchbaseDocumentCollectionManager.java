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
package org.eclipse.jnosql.communication.couchbase.document;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchQuery;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.ID_FIELD;
import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.convert;

/**
 * The default implementation of {@link CouchbaseDocumentCollectionManager}
 */
class DefaultCouchbaseDocumentCollectionManager implements CouchbaseDocumentCollectionManager {
    private final Bucket bucket;
    private final String database;

    private final Cluster cluster;

    DefaultCouchbaseDocumentCollectionManager(Cluster cluster, String database) {
        this.bucket = cluster.bucket(database);
        this.database = database;
        this.cluster = cluster;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        JsonObject json = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        Collection collection = bucket.collection(entity.getName());
        collection.insert(id.get(String.class), json);
        return entity;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        JsonObject json = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        Collection collection = bucket.collection(entity.getName());
        collection.insert(id.get(String.class), json, InsertOptions.insertOptions().expiry(ttl));
        return entity;
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
        JsonObject json = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        Collection collection = bucket.collection(entity.getName());
        collection.upsert(id.get(String.class), json);
        return entity;
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update).collect(Collectors.toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");


        Collection collection = bucket.collection(query.getDocumentCollection());

//        QueryConverter.QueryConverterResult delete = QueryConverter.delete(query, database);
//        if (nonNull(delete.getStatement())) {
//            ParameterizedN1qlQuery n1qlQuery = N1qlQuery.parameterized(delete.getStatement(), delete.getParams());
//            bucket.query(n1qlQuery);
//        }
//
//        if (!delete.getKeys().isEmpty()) {
//            delete.getKeys()
//                    .stream()
//                    .map(s -> getPrefix(query.getDocumentCollection(), s))
//                    .forEach(bucket::remove);
//        }

    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        N1QLBuilder n1QLBuilder = new N1QLBuilder(query, database);
        N1QLQuery n1QLQuery = n1QLBuilder.get();

        QueryResult result;
        if (n1QLQuery.isEmpty()) {
            result = cluster.query(n1QLQuery.getQuery());
        } else {
            result = cluster.query(n1QLQuery.getQuery(), QueryOptions
                    .queryOptions().parameters(n1QLQuery.getParams()));
        }
        List<JsonObject> jsons = result.rowsAsObject();
        return EntityConverter.convert(jsons, database);
    }

    @Override
    public long count(String documentCollection) {
        throw new UnsupportedOperationException("Couchbase does not support count method by document collection");
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery, JsonObject params) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        requireNonNull(params, "params is required");
//        N1qlQueryResult result = bucket.query(N1qlQuery.parameterized(n1qlQuery, params));
//        return convert(result, database);
        return null;
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
//        N1qlQueryResult result = bucket.query(N1qlQuery.simple(n1qlQuery));
//        return convert(result, database);
        return null;
    }


    @Override
    public Stream<DocumentEntity> search(SearchQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");

        return null;
    }

    @Override
    public void close() {
    }
}
