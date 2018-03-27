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
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.getMap;

/**
 * The Default implementation of {@link ElasticsearchDocumentCollectionManager}
 */
class DefaultElasticsearchDocumentCollectionManager implements ElasticsearchDocumentCollectionManager {


    private final RestHighLevelClient client;

    private final String index;

    DefaultElasticsearchDocumentCollectionManager(RestHighLevelClient client, String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        requireNonNull(entity, "entity is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        IndexRequest request = new IndexRequest(index, entity.getName(), id.get(String.class)).source(jsonObject);
        try {
            client.index(request);
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
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        return insert(entity);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");

        query.getCondition().orElseThrow(() -> new IllegalArgumentException("condition is required"));
        DocumentQuery select = new ElasticsearchDocumentQuery(query);

        List<DocumentEntity> entities = select(select);

        entities.stream()
                .map(entity -> entity.find(ID_FIELD).get().get(String.class))
                .forEach(id -> {
                    try {
                        client.delete(new DeleteRequest(index, query.getDocumentCollection(), id));
                    } catch (IOException e) {
                        throw new ElasticsearchException("An error to delete entities on elasticsearch", e);
                    }
                });

    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        return EntityConverter.query(query, client, index);
    }

    @Override
    public List<DocumentEntity> search(QueryBuilder query, String... types) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");

        try {
            SearchRequest searchRequest = new SearchRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(query);
            searchRequest.types(types);
            SearchResponse search = client.search(searchRequest);

            return stream(search.getHits().spliterator(), false)
                    .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.getSourceAsMap()))
                    .filter(ElasticsearchEntry::isNotEmpty)
                    .map(ElasticsearchEntry::toEntity)
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new ElasticsearchException("An error when do search from QueryBuilder on elasticsearch", e);
        }


    }

    @Override
    public void close() {
    }
}
