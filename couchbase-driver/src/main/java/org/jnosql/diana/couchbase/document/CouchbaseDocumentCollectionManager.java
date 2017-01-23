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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;
import com.couchbase.client.java.query.ParameterizedN1qlQuery;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.driver.value.ValueUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

/**
 * The couchbase implementation of {@link DocumentCollectionManager}
 */
public class CouchbaseDocumentCollectionManager implements DocumentCollectionManager {

    public static final String ID_FIELD = "_id";
    private final Bucket bucket;
    private final String database;

    CouchbaseDocumentCollectionManager(Bucket bucket, String database) {
        this.bucket = bucket;
        this.database = database;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        JsonObject jsonObject = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));
        String prefix = database + ':' + id.get(String.class);
        bucket.upsert(JsonDocument.create(prefix, jsonObject));
        return entity;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        JsonObject jsonObject = convert(entity);
        Document id = entity.find(ID_FIELD)
                .orElseThrow(() -> new CouchbaseNoKeyFoundException(entity.toString()));

        String prefix = database + ':' + id.get(String.class);
        Objects.requireNonNull(ttl, "ttl is required");
        bucket.upsert(JsonDocument.create(prefix, jsonObject), ttl.toMillis(), MILLISECONDS);
        return entity;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return save(entity);
    }

    @Override
    public void delete(DocumentQuery query) {
        QueryConverter.QueryConverterResult convert = QueryConverter.delete(query, database);
        ParameterizedN1qlQuery n1qlQuery = N1qlQuery.parameterized(convert.getStatement(), convert.getParams());
        N1qlQueryResult result = bucket.query(n1qlQuery);
    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {

        QueryConverter.QueryConverterResult select = QueryConverter.select(query, database);
        ParameterizedN1qlQuery n1qlQuery = N1qlQuery.parameterized(select.getStatement(), select.getParams());
        N1qlQueryResult result = bucket.query(n1qlQuery);
        return result.allRows().stream()
                .map(N1qlQueryRow::value)
                .map(JsonObject::toMap)
                .map(m -> (Map<String, Object>) m.get(database))
                .filter(Objects::nonNull)
                .map(Documents::of)
                .map(ds -> DocumentEntity.of(query.getCollection(), ds))
                .collect(toList());
    }

    @Override
    public void close() {
        bucket.close();
    }

    private JsonObject convert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        JsonObject jsonObject = JsonObject.create();
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
