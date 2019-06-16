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
import org.elasticsearch.action.search.SearchResponse;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.document.DocumentEntity;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.stream.StreamSupport.stream;

class FindQueryBuilderListener implements ActionListener<SearchResponse> {

    private final Consumer<List<DocumentEntity>> callBack;


    FindQueryBuilderListener(Consumer<List<DocumentEntity>> callBack) {
        this.callBack = callBack;
    }

    @Override
    public void onResponse(SearchResponse searchResponse) {
        List<DocumentEntity> entities = stream(searchResponse.getHits().spliterator(), false)
                .map(ElasticsearchEntry::of)
                .filter(ElasticsearchEntry::isNotEmpty)
                .map(ElasticsearchEntry::toEntity)
                .collect(Collectors.toList());
        callBack.accept(entities);
    }

    @Override
    public void onFailure(Exception e) {
        throw new ExecuteAsyncQueryException("An erro to execute a query from QueryBuilder on Elasticsearch", e);
    }
}
