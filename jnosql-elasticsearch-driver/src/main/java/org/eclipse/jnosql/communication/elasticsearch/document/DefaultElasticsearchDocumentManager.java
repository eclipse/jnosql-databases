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
package org.eclipse.jnosql.communication.elasticsearch.document;


import jakarta.nosql.CommunicationException;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;

/**
 * The Default implementation of {@link ElasticsearchDocumentManager}
 */
class DefaultElasticsearchDocumentManager implements ElasticsearchDocumentManager {


    private final RestHighLevelClient client;

    private final String index;

    DefaultElasticsearchDocumentManager(RestHighLevelClient client, String index) {
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
        IndexRequest request = new IndexRequest(index).id(id.get(String.class)).source(jsonObject);
        try {
            client.index(request, RequestOptions.DEFAULT);
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

        BulkRequest bulk = new BulkRequest();

        entities.stream()
                .map(entity -> entity.find(EntityConverter.ID_FIELD).get().get(String.class))
                .map(id -> new DeleteRequest(index, id))
                .forEach(bulk::add);

        try {
            client.bulk(bulk, RequestOptions.DEFAULT);
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
        Objects.requireNonNull(documentCollection, "query is required");
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0);
        try {
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
            return search.getHits().getTotalHits().value;
        } catch (IOException e) {
            throw new CommunicationException("Error on ES when try to execute count to document collection:" + documentCollection, e);
        }
    }

    @Override
    public Stream<DocumentEntity> search(QueryBuilder query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");

        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(query);
            searchRequest.source(searchSourceBuilder);
            SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);

            return stream(search.getHits().spliterator(), false)
                    .map(ElasticsearchEntry::of)
                    .filter(ElasticsearchEntry::isNotEmpty)
                    .map(ElasticsearchEntry::toEntity);
        } catch (IOException e) {
            throw new ElasticsearchException("An error when do search from QueryBuilder on elasticsearch", e);
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException("An error when close the client", e);
        }
    }
}
