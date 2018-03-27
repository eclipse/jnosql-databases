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


import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

final class EntityConverter {

    static final String ID_FIELD = "_id";


    private EntityConverter() {
    }


    static Map<String, Object> getMap(DocumentEntity entity) {
        Map<String, Object> jsonObject = new HashMap<>();

        entity.getDocuments().stream()
                .filter(d -> !d.getName().equals(ID_FIELD))
                .forEach(feedJSON(jsonObject));
        return jsonObject;
    }

    static List<DocumentEntity> query(DocumentQuery query, RestHighLevelClient client, String index) {
        QueryConverterResult select = QueryConverter.select(query);


        try {
            List<DocumentEntity> entities = new ArrayList<>();

            if (select.hasId()) {
                executeId(query, client, index, select, entities);
            }
            if (select.hasStatement()) {
                executeStatement(query, client, index, select, entities);
            }


            return entities;
        } catch (IOException e) {
            throw new ElasticsearchException("An error to execute a query on elasticsearch", e);
        }
    }

    private static void executeStatement(DocumentQuery query, RestHighLevelClient client, String index,
                                         QueryConverterResult select,
                                         List<DocumentEntity> entities) throws IOException {


        SearchRequest searchRequest = new SearchRequest(index);
        searchRequest.types(query.getDocumentCollection());
        if (select.hasQuery()) {
            setQueryBuilder(query, select, searchRequest);
        }

        SearchResponse response = client.search(searchRequest);
        Stream.of(response.getHits())
                .flatMap(h -> Stream.of(h.getHits()))
                .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.getSourceAsMap()))
                .filter(ElasticsearchEntry::isNotEmpty)
                .map(ElasticsearchEntry::toEntity)
                .forEach(entities::add);
    }


    static void queryAsync(DocumentQuery query, RestHighLevelClient client, String index,
                           Consumer<List<DocumentEntity>> callBack) {

        FindAsyncListener listener = new FindAsyncListener(callBack, query.getDocumentCollection());
        QueryConverterResult select = QueryConverter.select(query);

        if (!select.getIds().isEmpty()) {
            MultiGetRequest multiGetRequest = new MultiGetRequest();

            select.getIds().stream()
                    .map(id -> new MultiGetRequest.Item(index, query.getDocumentCollection(), id))
                    .forEach(multiGetRequest::add);
            client.multiGetAsync(multiGetRequest, listener.getIds());

        }

        if (select.hasStatement()) {
            SearchRequest searchRequest = new SearchRequest(index);
            searchRequest.types(query.getDocumentCollection());
            if (select.hasQuery()) {
                setQueryBuilder(query, select, searchRequest);
            }
            client.searchAsync(searchRequest, listener.getSearch());
        }

    }

    private static Consumer<Document> feedJSON(Map<String, Object> jsonObject) {
        return d -> {
            Object value = ValueUtil.convert(d.getValue());
            if (value instanceof Document) {
                Document subDocument = Document.class.cast(value);
                jsonObject.put(d.getName(), singletonMap(subDocument.getName(), subDocument.get()));
            } else if (isSudDocument(value)) {
                Map<String, Object> subDocument = getMap(value);
                jsonObject.put(d.getName(), subDocument);
            } else if (isSudDocumentList(value)) {
                jsonObject.put(d.getName(), StreamSupport.stream(Iterable.class.cast(value).spliterator(), false)
                        .map(EntityConverter::getMap).collect(toList()));
            } else {
                jsonObject.put(d.getName(), value);
            }
        };
    }

    private static Map<String, Object> getMap(Object value) {
        Map<String, Object> subDocument = new HashMap<>();
        StreamSupport.stream(Iterable.class.cast(value).spliterator(),
                false).forEach(feedJSON(subDocument));
        return subDocument;
    }

    private static boolean isSudDocument(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(org.jnosql.diana.api.document.Document.class::isInstance);
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }

    private static void executeId(DocumentQuery query, RestHighLevelClient client, String index,
                                  QueryConverterResult select,
                                  List<DocumentEntity> entities) throws IOException {

        String type = query.getDocumentCollection();
        MultiGetRequest multiGetRequest = new MultiGetRequest();

        select.getIds().stream()
                .map(id -> new MultiGetRequest.Item(index, type, id))
                .forEach(multiGetRequest::add);

        MultiGetResponse responses = client.multiGet(multiGetRequest);
        Stream.of(responses.getResponses())
                .map(MultiGetItemResponse::getResponse)
                .map(g -> new ElasticsearchEntry(g.getId(), g.getIndex(), g.getSourceAsMap()))
                .filter(ElasticsearchEntry::isNotEmpty)
                .map(ElasticsearchEntry::toEntity)
                .forEach(entities::add);

    }

    private static void setQueryBuilder(DocumentQuery query, QueryConverterResult select, SearchRequest searchRequest) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(select.getStatement());
        searchRequest.source(searchSourceBuilder);
        int from = (int) query.getFirstResult();
        int size = (int) query.getMaxResults();
        if (from > 0) {
            searchSourceBuilder.from(from);
        }
        if (size > 0) {
            searchSourceBuilder.size(size);
        }
    }


}
