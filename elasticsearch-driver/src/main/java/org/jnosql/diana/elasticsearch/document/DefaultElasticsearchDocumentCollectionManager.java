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


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.query.DocumentQueryBuilder;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.getMap;

/**
 * The Default implementation of {@link ElasticsearchDocumentCollectionManager}
 */
class DefaultElasticsearchDocumentCollectionManager implements ElasticsearchDocumentCollectionManager {


    protected static final Jsonb JSONB = JsonbBuilder.create();

    private final Client client;

    private final String index;

    DefaultElasticsearchDocumentCollectionManager(Client client, String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = JSONB.toJson(jsonObject).getBytes(UTF_8);
        try {
            client.prepareIndex(index, entity.getName(), id.get(String.class)).setSource(bytes)
                    .execute().get();
            return entity;
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to try to save/update entity on elasticsearch", e);
        }

    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = JSONB.toJson(jsonObject).getBytes(UTF_8);
        try {
            client.prepareIndex(index, entity.getName(), id.get(String.class))
                    .setSource(bytes)
                    .setTTL(timeValueMillis(ttl.toMillis()))
                    .execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to try to save with TTL entity on elasticsearch", e);
        }
        return entity;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        return insert(entity);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");

        DocumentQuery select = DocumentQueryBuilder.select().from(query.getDocumentCollection())
                .where(query.getCondition().orElseThrow(() -> new IllegalArgumentException("condition is required")))
                .build();

        List<DocumentEntity> entities = select(select);

        entities.stream()
                .map(entity -> entity.find(ID_FIELD).get().get(String.class))
                .forEach(id -> {
                    try {
                        client.prepareDelete(index, query.getDocumentCollection(), id).execute().get();
                    } catch (InterruptedException | ExecutionException e) {
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

        SearchResponse searchResponse = null;
        try {
            searchResponse = client.prepareSearch(index)
                    .setTypes(types)
                    .setQuery(query)
                    .execute().get();

            return stream(searchResponse.getHits().spliterator(), false)
                    .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.sourceAsMap()))
                    .filter(ElasticsearchEntry::isNotEmpty)
                    .map(ElasticsearchEntry::toEntity)
                    .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error when do search from QueryBuilder on elasticsearch", e);
        }


    }

    @Override
    public void close() {

    }
}
