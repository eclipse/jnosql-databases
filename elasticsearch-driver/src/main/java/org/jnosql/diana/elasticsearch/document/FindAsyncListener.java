package org.jnosql.diana.elasticsearch.document;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.synchronizedList;
import static java.util.stream.StreamSupport.stream;

class FindAsyncListener {

    private final Consumer<List<DocumentEntity>> callBack;

    private final String collection;

    private final List<DocumentEntity> entities = synchronizedList(new ArrayList<>());

    private AtomicBoolean ids = new AtomicBoolean(true);

    private AtomicBoolean query = new AtomicBoolean(true);

    FindAsyncListener(Consumer<List<DocumentEntity>> callBack, String collection) {
        this.callBack = callBack;
        this.collection = collection;
    }

    ActionListener<MultiGetResponse> getIds() {
        ids.set(false);
        return new ActionListener<MultiGetResponse>() {
            @Override
            public void onResponse(MultiGetResponse multiGetItemResponses) {
                ids.set(true);

                Stream.of(multiGetItemResponses.getResponses())
                        .map(MultiGetItemResponse::getResponse)
                        .map(h -> new ElasticsearchEntry(h.getId(), collection, h.getSourceAsMap()))
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
                if (ids.get() && query.get()) {
                    callBack.accept(entities);
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An error when execute query", e);
            }
        };
    }

    ActionListener<SearchResponse> getSearch() {
        query.set(false);
        return new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                query.set(true);
                stream(searchResponse.getHits().spliterator(), false)
                        .map(h -> new ElasticsearchEntry(h.getId(), collection, h.sourceAsMap()))
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
                if (ids.get() && query.get()) {
                    callBack.accept(entities);
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An error when execute query", e);
            }
        };
    }

}
