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
package org.eclipse.jnosql.diana.elasticsearch.document;

import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.document.DocumentEntity;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.search.SearchResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static java.util.Collections.synchronizedList;
import static java.util.stream.StreamSupport.stream;

final class FindAsyncListener {

    private final Consumer<Stream<DocumentEntity>> callBack;

    private final String collection;

    private final List<DocumentEntity> entities = synchronizedList(new ArrayList<>());

    private AtomicBoolean ids = new AtomicBoolean(true);

    private AtomicBoolean query = new AtomicBoolean(true);

    FindAsyncListener(Consumer<Stream<DocumentEntity>> callBack, String collection) {
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
                        .map(ElasticsearchEntry::of)
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
                if (ids.get() && query.get()) {
                    callBack.accept(entities.stream());
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
                        .map(ElasticsearchEntry::of)
                        .filter(ElasticsearchEntry::isNotEmpty)
                        .map(ElasticsearchEntry::toEntity)
                        .forEach(entities::add);
                if (ids.get() && query.get()) {
                    callBack.accept(entities.stream());
                }
            }

            @Override
            public void onFailure(Exception e) {
                throw new ExecuteAsyncQueryException("An error when execute query", e);
            }
        };
    }

}
