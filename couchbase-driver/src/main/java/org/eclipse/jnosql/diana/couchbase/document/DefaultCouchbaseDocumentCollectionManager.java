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
package org.eclipse.jnosql.diana.couchbase.document;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.search.SearchQuery;
import com.couchbase.client.java.search.result.SearchQueryResult;
import com.couchbase.client.java.search.result.SearchQueryRow;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.diana.couchbase.document.EntityConverter.ID_FIELD;
import static org.eclipse.jnosql.diana.couchbase.document.EntityConverter.KEY_FIELD;
import static org.eclipse.jnosql.diana.couchbase.document.EntityConverter.convert;
import static org.eclipse.jnosql.diana.couchbase.document.EntityConverter.getPrefix;

/**
 * The default implementation of {@link CouchbaseDocumentCollectionManager}
 */
class DefaultCouchbaseDocumentCollectionManager implements CouchbaseDocumentCollectionManager {
    private final Bucket bucket;
    private final String database;

    DefaultCouchbaseDocumentCollectionManager(Bucket bucket, String database) {
        this.bucket = bucket;
        this.database = database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        JsonObject jsonObject = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        String prefix = getPrefix(id, entity.getName());
        jsonObject.put(KEY_FIELD, prefix);
        bucket.upsert(JsonDocument.create(prefix, jsonObject));
        entity.add(Document.of(ID_FIELD, prefix));
        return entity;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        JsonObject jsonObject = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        String prefix = getPrefix(id, entity.getName());
        jsonObject.put(KEY_FIELD, prefix);
        bucket.upsert(JsonDocument.create(prefix, (int) ttl.getSeconds(), jsonObject));
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
        return insert(entity);
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update).collect(Collectors.toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        QueryConverter.QueryConverterResult delete = QueryConverter.delete(query, database);
        if (nonNull(delete.getStatement())) {
            ParameterizedN1qlQuery n1qlQuery = N1qlQuery.parameterized(delete.getStatement(), delete.getParams());
            bucket.query(n1qlQuery);
        }
        if (!delete.getKeys().isEmpty()) {
            delete.getKeys()
                    .stream()
                    .map(s -> getPrefix(query.getDocumentCollection(), s))
                    .forEach(bucket::remove);
        }

    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) throws NullPointerException {

        QueryConverter.QueryConverterResult select = QueryConverter.select(query, database);
        Stream<DocumentEntity> idsQuery = Stream.empty();
        Stream<DocumentEntity> n1qlQueryStream = Stream.empty();
        if (nonNull(select.getStatement())) {
            ParameterizedN1qlQuery n1qlQuery = N1qlQuery.parameterized(select.getStatement(), select.getParams());
            N1qlQueryResult result = bucket.query(n1qlQuery);
            idsQuery = convert(result, database);
        }
        if (!select.getKeys().isEmpty()) {
            idsQuery = convert(select.getKeys().stream(), bucket);
        }

        return Stream.concat(n1qlQueryStream, idsQuery);
    }

    @Override
    public long count(String documentCollection) {
        throw new UnsupportedOperationException("Couchbase does not support count method by document collection");
    }


    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery, JsonObject params) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        requireNonNull(params, "params is required");
        N1qlQueryResult result = bucket.query(N1qlQuery.parameterized(n1qlQuery, params));
        return convert(result, database);
    }

    @Override
    public Stream<DocumentEntity> n1qlQuery(Statement n1qlQuery, JsonObject params) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        requireNonNull(params, "params is required");
        N1qlQueryResult result = bucket.query(N1qlQuery.parameterized(n1qlQuery, params));
        return convert(result, database);
    }

    @Override
    public Stream<DocumentEntity> n1qlQuery(String n1qlQuery) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        N1qlQueryResult result = bucket.query(N1qlQuery.simple(n1qlQuery));
        return convert(result, database);
    }

    @Override
    public Stream<DocumentEntity> n1qlQuery(Statement n1qlQuery) throws NullPointerException {
        requireNonNull(n1qlQuery, "n1qlQuery is required");
        N1qlQueryResult result = bucket.query(N1qlQuery.simple(n1qlQuery));
        return convert(result, database);
    }

    @Override
    public Stream<DocumentEntity> search(SearchQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        SearchQueryResult result = bucket.query(query);
        Stream<String> keys = StreamSupport.stream(result.spliterator(), false)
                .map(SearchQueryRow::id);
        return convert(keys, bucket);
    }

    @Override
    public void close() {
        bucket.close();
    }
}
