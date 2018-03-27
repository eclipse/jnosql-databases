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


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.getMap;

class DefaultElasticsearchDocumentCollectionManagerAsync implements ElasticsearchDocumentCollectionManagerAsync {

    protected static final Jsonb JSONB = JsonbBuilder.create();

    private static final Consumer<DocumentEntity> NOOP = e -> {
    };

    private final RestHighLevelClient client;
    private final String index;

    DefaultElasticsearchDocumentCollectionManagerAsync(RestHighLevelClient client, String index) {
        this.client = client;
        this.index = index;
    }

    @Override
    public void insert(DocumentEntity entity) {
        insert(entity, NOOP);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) {
        insert(entity, ttl, e -> {
        });
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        IndexRequest request = new IndexRequest(index, entity.getName(), id.get(String.class)).source(jsonObject);
        client.indexAsync(request, new SaveActionListener(callBack, entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("The insert with TTL does not support");
    }

    @Override
    public void update(DocumentEntity entity) {
        insert(entity);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) {
        insert(entity, callBack);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, d -> {
        });

    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");


        query.getCondition().orElseThrow(() -> new IllegalArgumentException("condition is required"));
        DocumentQuery select = new ElasticsearchDocumentQuery(query);

        List<DocumentEntity> entities = EntityConverter.query(select, client, index);
        BulkRequest bulk = new BulkRequest();

        entities.stream()
                .map(entity -> entity.find(ID_FIELD).get().get(String.class))
                .map(id -> new DeleteRequest(index, query.getDocumentCollection(), id))
                .forEach(bulk::add);


        ActionListener<BulkResponse> listener = new ActionListener<BulkResponse>() {
            @Override
            public void onResponse(BulkResponse bulkItemResponses) {
                callBack.accept(null);
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An error when delete on elasticsearch", e);
            }
        };
        
        client.bulkAsync(bulk, listener);
    }

    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        EntityConverter.queryAsync(query, client, index, callBack);
    }


    @Override
    public void search(QueryBuilder query, Consumer<List<DocumentEntity>> callBack, String... types) {
        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(query);
        searchRequest.types(types);
        client.searchAsync(searchRequest, new FindQueryBuilderListener(callBack));
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
