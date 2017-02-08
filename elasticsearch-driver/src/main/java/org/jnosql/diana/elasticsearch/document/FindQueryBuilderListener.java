/*
 * Copyright 2017 Eclipse Foundation
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
import org.elasticsearch.action.search.SearchResponse;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentEntity;

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
                .map(h -> new ElasticsearchEntry(h.getId(), h.getIndex(), h.sourceAsMap()))
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
