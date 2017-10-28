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
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonMap;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

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
                allMatch(d -> org.jnosql.diana.api.document.Document.class.isInstance(d));
    }

    private static boolean isSudDocumentList(Object value) {
        return value instanceof Iterable && StreamSupport.stream(Iterable.class.cast(value).spliterator(), false).
                allMatch(d -> d instanceof Iterable && isSudDocument(d));
    }

    static List<DocumentEntity> query(DocumentQuery query, Client client, String index) {
        QueryConverter.QueryConverterResult select = QueryConverter.select(query);


        try {
            List<DocumentEntity> entities = new ArrayList<>();

            if (select.hasId()) {
                executeId(query, client, index, select, entities);
            }
            if (select.hasStatement()) {
                executeStatement(query, client, index, select, entities);
            }


            return entities;
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to execute a query on elasticsearch", e);
        }
    }

    private static void executeStatement(DocumentQuery query, Client client, String index,
                                         QueryConverter.QueryConverterResult select,
                                         List<DocumentEntity> entities)
            throws InterruptedException, ExecutionException {

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index)
                .setTypes(query.getDocumentCollection());
        if (select.hasQuery()) {
            searchRequestBuilder.setQuery(select.getStatement());
        }

        SearchResponse searchResponse = searchRequestBuilder.execute().get();
        stream(searchResponse.getHits().spliterator(), false)
                .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.sourceAsMap()))
                .filter(ElasticsearchEntry::isNotEmpty)
                .map(ElasticsearchEntry::toEntity)
                .forEach(entities::add);
    }

    private static void executeId(DocumentQuery query, Client client, String index,
                                  QueryConverter.QueryConverterResult select,
                                  List<DocumentEntity> entities) throws InterruptedException, ExecutionException {

        MultiGetResponse multiGetItemResponses = client
                .prepareMultiGet().add(index, query.getDocumentCollection(), select.getIds())
                .execute().get();

        Stream.of(multiGetItemResponses.getResponses())
                .map(MultiGetItemResponse::getResponse)
                .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.getSourceAsMap()))
                .filter(ElasticsearchEntry::isNotEmpty)
                .map(ElasticsearchEntry::toEntity)
                .forEach(entities::add);
    }

    static void queryAsync(DocumentQuery query, Client client, String index, Consumer<List<DocumentEntity>> callBack) {

        FindAsyncListener listener = new FindAsyncListener(callBack, query.getDocumentCollection());
        QueryConverter.QueryConverterResult select = QueryConverter.select(query);

        if (!select.getIds().isEmpty()) {
            client.prepareMultiGet().add(index, query.getDocumentCollection(), select.getIds())
                    .execute().addListener(listener.getIds());


        }
        if (nonNull(select.getStatement())) {
            client.prepareSearch(index)
                    .setTypes(query.getDocumentCollection())
                    .setQuery(select.getStatement())
                    .execute().addListener(listener.getSearch());
        }

    }
}
