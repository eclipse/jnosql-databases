package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.value.ValueUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Objects.nonNull;
import static java.util.stream.StreamSupport.stream;

final class EntityConverter {

    static final String ID_FIELD = "_id";


    private EntityConverter() {
    }


    static Map<String, Object> getMap(DocumentEntity entity) {
        Map<String, Object> jsonObject = new java.util.HashMap<>();

        entity.getDocuments().stream()
                .filter(d -> !d.getName().equals(ID_FIELD))
                .forEach(d -> {
                    Object value = ValueUtil.convert(d.getValue());
                    if (Document.class.isInstance(value)) {
                        Document document = Document.class.cast(value);
                        jsonObject.put(d.getName(), Collections.singletonMap(document.getName(), document.get()));
                    } else {
                        jsonObject.put(d.getName(), value);
                    }
                });
        return jsonObject;
    }

    static List<DocumentEntity> query(DocumentQuery query, Client client, String index) {
        QueryConverter.QueryConverterResult select = QueryConverter.select(query);


        try {
            List<DocumentEntity> entities = new ArrayList<>();

            if (!select.getIds().isEmpty()) {
                MultiGetResponse multiGetItemResponses = client
                        .prepareMultiGet().add(index, query.getCollection(), select.getIds())
                        .execute().get();

                Stream.of(multiGetItemResponses.getResponses())
                        .map(MultiGetItemResponse::getResponse)
                        .map(h -> new ElasticsearchEntry(h.getId(), query.getCollection(), h.getSourceAsMap()))
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
            }
            if (nonNull(select.getStatement())) {
                SearchResponse searchResponse = client.prepareSearch(index)
                        .setTypes(query.getCollection())
                        .setQuery(select.getStatement())
                        .execute().get();
                stream(searchResponse.getHits().spliterator(), false)
                        .map(h -> new ElasticsearchEntry(h.getId(), query.getCollection(), h.sourceAsMap()))
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
            }


            return entities;
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to execute a query on elasticsearch", e);
        }
    }

    static void queryAsync(DocumentQuery query, Client client, String index, Consumer<List<DocumentEntity>> callBack) {

        FindAsyncListener listener = new FindAsyncListener(callBack, query.getCollection());
        QueryConverter.QueryConverterResult select = QueryConverter.select(query);

        if (!select.getIds().isEmpty()) {
            client.prepareMultiGet().add(index, query.getCollection(), select.getIds())
                    .execute().addListener(listener.getIds());


        }
        if (nonNull(select.getStatement())) {
            client.prepareSearch(index)
                    .setTypes(query.getCollection())
                    .setQuery(select.getStatement())
                    .execute().addListener(listener.getSearch());
        }

    }
}
