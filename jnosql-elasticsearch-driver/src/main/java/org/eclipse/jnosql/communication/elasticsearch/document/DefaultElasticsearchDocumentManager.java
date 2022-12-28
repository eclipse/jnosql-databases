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
 *   Maximillian Arruda
 */
package org.eclipse.jnosql.communication.elasticsearch.document;


import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.MatchQuery;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import jakarta.nosql.CommunicationException;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The Default implementation of {@link ElasticsearchDocumentManager}
 */
class DefaultElasticsearchDocumentManager implements ElasticsearchDocumentManager {


    private final ElasticsearchClient client;

    private final String index;

    public DefaultElasticsearchDocumentManager(ElasticsearchClient client, String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public String getName() {
        return index;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");
        Document id = entity.find(EntityConverter.ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = EntityConverter.getMap(entity);
        try {
            var indexRequest = IndexRequest.of(b ->
                    b.index(index)
                            .id(id.get(String.class)).document(jsonObject)
            );
            client.index(indexRequest);
        } catch (IOException e) {
            throw new ElasticsearchException("An error to insert in Elastic search", e);
        }
        return entity;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("The insert with TTL does not support");
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
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        return insert(entity);
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update).collect(Collectors.toList());
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");

        DocumentQuery select = new ElasticsearchDocumentQuery(query);

        List<DocumentEntity> entities = select(select).collect(Collectors.toList());

        if (entities.isEmpty()) {
            return;
        }

        BulkRequest.Builder bulkRequest = new BulkRequest.Builder();

        entities.stream()
                .map(entity -> entity.find(EntityConverter.ID_FIELD).orElseThrow().get(String.class))
                .forEach(id ->
                        bulkRequest.operations(op -> op
                                .delete(DeleteOperation.of(dp -> dp
                                        .index(index)
                                        .id(id)))
                        )
                );
        try {
            client.bulk(bulkRequest.build());
        } catch (IOException e) {
            throw new ElasticsearchException("An error to delete entities on elasticsearch", e);
        }
    }


    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        return EntityConverter.query(query, client, index);
    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");
        try {
            return client.count(CountRequest.of(s -> s.index(index)
                    .query(MatchQuery.of(m -> m
                            .field(EntityConverter.ENTITY)
                            .query(documentCollection))._toQuery()))).count();
        } catch (IOException e) {
            throw new CommunicationException("Error on ES when try to execute count to document collection:" + documentCollection, e);
        }
    }

    @Override
    public Stream<DocumentEntity> search(SearchRequest query) {
        Objects.requireNonNull(query, "query is required");
        try {
            var responses = client.search(query, Map.class);
            return EntityConverter.getDocumentEntityStream(client, responses);
        } catch (IOException e) {
            throw new ElasticsearchException("An error when do search from QueryBuilder on elasticsearch", e);
        }
    }

    @Override
    public void close() {
        try {
            client._transport().close();
        } catch (IOException e) {
            throw new ElasticsearchException("An error when close the client", e);
        }
    }
}
