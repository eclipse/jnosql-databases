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
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class ElasticsearchDocumentCollectionManager implements DocumentCollectionManager {

    private final Client client;

    ElasticsearchDocumentCollectionManager(Client client) {
        this.client = client;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        return null;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        return null;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        return null;
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
}
