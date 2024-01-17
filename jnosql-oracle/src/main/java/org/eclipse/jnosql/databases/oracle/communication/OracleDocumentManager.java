/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import jakarta.json.bind.Jsonb;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

final class OracleDocumentManager implements DocumentManager {
    static final String ENTITY = "_entity";
    static final String ID = "_id";
    private final String table;
    private final NoSQLHandle serviceHandle;

    private final Jsonb jsonB;
    public OracleDocumentManager(String table, NoSQLHandle serviceHandle, Jsonb jsonB) {
        this.table = table;
        this.serviceHandle = serviceHandle;
        this.jsonB = jsonB;
    }

    @Override
    public String name() {
        return table;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        Map<String, Object> entityMap = new HashMap<>(entity.toMap());
        entityMap.put(ENTITY, entity.name());
        String id = entity.find(ID).map(Document::get)
                .map(Object::toString)
                .orElseThrow(() -> new OracleDBException("The _id is required in the entity"));

        MapValue mapValue = new MapValue().put("id", id);
        MapValue contentVal = mapValue.putFromJson("content", jsonB.toJson(entityMap),
                new JsonOptions());
        PutRequest putRequest = new PutRequest()
                .setValue(contentVal)
                .setTableName(name());

        serviceHandle.put(putRequest);
        return entity;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
       throw new UnsupportedOperationException("Oracle NoSQL database does not support TTL");
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        StreamSupport.stream(entities.spliterator(), false).forEach(this::insert);
        return entities;
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        throw new UnsupportedOperationException("Oracle NoSQL database does not support TTL");
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        return insert(entity);
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        return insert(entities);
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");

    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        return null;
    }

    @Override
    public long count(String documentCollection) {
        return 0;
    }

    @Override
    public void close() {

    }
}
