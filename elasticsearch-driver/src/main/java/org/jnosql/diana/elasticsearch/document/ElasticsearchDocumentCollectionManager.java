/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.getMap;

public class ElasticsearchDocumentCollectionManager implements DocumentCollectionManager {


    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();

    private final Client client;

    private final String index;

    ElasticsearchDocumentCollectionManager(Client client, String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
        try {
            client.prepareIndex(index, entity.getName(), id.get(String.class)).setSource(bytes)
                    .execute().get();
            return entity;
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to try to save/update entity on elasticsearch", e);
        }

    }


    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
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
        return save(entity);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        List<DocumentEntity> entities = find(DocumentQuery.of(query.getCollection())
                .and(query.getCondition().orElseThrow(() -> new IllegalArgumentException("condition is required"))));

        entities.stream()
                .map(entity -> entity.find(ID_FIELD).get().get(String.class))
                .forEach(id -> {
                    try {
                        client.prepareDelete(index, query.getCollection(), id).execute().get();
                    } catch (InterruptedException | ExecutionException e) {
                        throw new ElasticsearchException("An error to delete entities on elasticsearch", e);
                    }
                });

    }


    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        return EntityConverter.query(query, client, index);
    }

    /**
     * Find entities from {@link QueryBuilder}
     *
     * @param query the query
     * @param types the types
     * @return the objects from query
     * @throws NullPointerException when query is null
     */
    public List<DocumentEntity> find(QueryBuilder query, String... types) throws NullPointerException {
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
            throw new ElasticsearchException("An error when do find from QueryBuilder on elasticsearch", e);
        }


    }

    @Override
    public void close() {

    }

}
