/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.StreamSupport.stream;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.getMap;

public class ElasticsearchDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();
    private static final Consumer<DocumentEntity> NOOP = e -> {
    };

    private final Client client;
    private final String index;

    ElasticsearchDocumentCollectionManagerAsync(Client client, String index) {

        this.client = client;
        this.index = index;
    }

    @Override
    public void save(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        save(entity, NOOP);
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        save(entity, ttl, e -> {
        });
    }

    @Override
    public void save(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
        client.prepareIndex(index, entity.getName(), id.get(String.class)).setSource(bytes).execute()
                .addListener(new SaveActionListener(callBack, entity));


    }

    @Override
    public void save(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        requireNonNull(callBack, "callBack is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
        client.prepareIndex(index, entity.getName(), id.get(String.class)).setSource(bytes).
                setTTL(timeValueMillis(ttl.toMillis())).execute()
                .addListener(new SaveActionListener(callBack, entity));

    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        save(entity);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        save(entity, callBack);
    }

    @Override
    public void delete(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, d -> {
        });

    }

    @Override
    public void delete(DocumentQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        List<DocumentEntity> entities = EntityConverter.query(query, client, index);
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        entities.stream()
                .map(entity -> entity.find(ID_FIELD).get().get(String.class))
                .map(id -> client.prepareDelete(index, query.getCollection(), id))
                .forEach(bulkRequest::add);

        ActionListener<BulkResponse> s = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                callBack.accept(null);
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An error when delete on elasticsearch", e);
            }
        };
        bulkRequest.execute().addListener(s);
    }

    @Override
    public void find(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException, NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        EntityConverter.queryAsync(query, client, index, callBack);
    }


    /**
     * Find entities from {@link QueryBuilder}
     *
     * @param query    the query
     * @param type     the type
     * @param callBack the callback
     * @return the objects from query
     * @throws NullPointerException when query is null
     */
    public void find(QueryBuilder query, String type, Consumer<List<DocumentEntity>> callBack) throws NullPointerException, ExecuteAsyncQueryException {
        Objects.requireNonNull(query, "query is required");

        client.prepareSearch(index)
                .setTypes(type)
                .setQuery(query)
                .execute().addListener(new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                List<DocumentEntity> entities = stream(searchResponse.getHits().spliterator(), false)
                        .map(h -> new ElasticsearchEntry(h.getId(), type, h.sourceAsMap()))
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .collect(Collectors.toList());
                callBack.accept(entities);
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An erro to execute a query from QueryBuilder on Elasticsearch", e);
            }
        });


    }

    @Override
    public void close() {

    }
}
