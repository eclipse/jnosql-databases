/*
 * Copyright 2017 Otavio Santana and others
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

import org.elasticsearch.action.index.IndexResponse;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentEntity;

import java.util.function.Consumer;

class SaveActionListener implements org.elasticsearch.action.ActionListener<IndexResponse> {

    private final Consumer<DocumentEntity> callBack;

    private final DocumentEntity entity;

    SaveActionListener(Consumer<DocumentEntity> callBack, DocumentEntity entity) {
        this.callBack = callBack;
        this.entity = entity;
    }

    @Override
    public void onResponse(IndexResponse indexResponse) {
        callBack.accept(entity);
    }

    @Override
    public void onFailure(Exception e) {
        throw new ExecuteAsyncQueryException("An error when execute async elasticsearch query", e);
    }
}
