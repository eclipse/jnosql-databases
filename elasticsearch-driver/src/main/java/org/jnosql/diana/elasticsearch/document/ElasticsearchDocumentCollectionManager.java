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


import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;
import org.jnosql.diana.driver.value.ValueUtil;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.util.Objects.requireNonNull;
import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

public class ElasticsearchDocumentCollectionManager implements DocumentCollectionManager {

    static final String ID_FIELD = "_id";

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();

    private final Client client;
    private final String database;

    ElasticsearchDocumentCollectionManager(Client client, String database) {
        this.client = client;
        this.database = database;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
        try {
            client.prepareIndex(database, entity.getName(), id.get(String.class)).setSource(bytes).execute().get();
            return entity;
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to try to save/update entity on elasticsearch", e);
        }

    }


    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new ElasticsearchKeyFoundException(entity.toString()));
        Map<String, Object> jsonObject = getMap(entity);
        byte[] bytes = PROVDER.toJsonArray(jsonObject);
        try {
            client.prepareIndex(database, entity.getName(), id.get(String.class))
                    .setSource(bytes)
                    .setTTL(timeValueMillis(ttl.toMillis()))
                    .execute().get();
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("An error to try to save with TTL entity on elasticsearch", e);
        }
        return entity;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        return save(entity);
    }

    @Override
    public void delete(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        return null;
    }

    @Override
    public void close() {

    }

    private Map<String, Object> getMap(DocumentEntity entity) {
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
}
