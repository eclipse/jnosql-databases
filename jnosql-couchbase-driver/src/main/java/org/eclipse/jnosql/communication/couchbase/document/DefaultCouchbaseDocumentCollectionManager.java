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


import com.couchbase.client.core.error.DocumentNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.kv.GetResult;
import com.couchbase.client.java.kv.InsertOptions;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.search.SearchQuery;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.COLLECTION_FIELD;
import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.ID_FIELD;
import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.convert;

/**
 * The default implementation of {@link CouchbaseDocumentCollectionManager}
 */
class DefaultCouchbaseDocumentCollectionManager implements CouchbaseDocumentCollectionManager {

    private static final Logger LOGGER = Logger.getLogger(DefaultCouchbaseDocumentCollectionManager.class.getName());

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
        entity.add(COLLECTION_FIELD, entity.getName());
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
        entity.add(COLLECTION_FIELD, entity.getName());
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
        N1QLQuery n1QLQuery = new N1QLBuilder(query, database).get();
        List<JsonObject> jsons = new ArrayList<>();

        if (n1QLQuery.hasIds()) {
            Collection collection = bucket.collection(query.getDocumentCollection());
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
    }

    @Override
    public long count(String documentCollection) {
        throw new UnsupportedOperationException("Couchbase does not support count method by document collection");
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery, JsonObject params) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        requireNonNull(params, "params is required");

        QueryResult query = cluster.query(n1qlQuery, QueryOptions
                .queryOptions().parameters(params));
        return EntityConverter.convert(query.rowsAsObject(), database);
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        QueryResult query = cluster.query(n1qlQuery);
        return EntityConverter.convert(query.rowsAsObject(), database);
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
