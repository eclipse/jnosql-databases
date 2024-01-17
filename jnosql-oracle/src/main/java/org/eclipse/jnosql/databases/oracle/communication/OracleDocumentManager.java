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

import jakarta.json.Json;
import jakarta.json.JsonObject;
import jakarta.json.JsonReader;
import jakarta.json.JsonValue;
import jakarta.json.bind.Jsonb;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.ID_FIELD;
import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.JSON_FIELD;

final class OracleDocumentManager implements DocumentManager {

    private static final JsonOptions OPTIONS = new JsonOptions();
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
        Objects.requireNonNull(query, "query is required");
        SelectBuilder selectBuilder = new SelectBuilder(query, table);
        OracleQuery oracleQuery = selectBuilder.get();
        List<DocumentEntity> entities = new ArrayList<>();

        if (oracleQuery.hasIds()) {
            entities.addAll(getIds(oracleQuery));
        }
        if (!oracleQuery.hasOnlyIds()) {
            if (oracleQuery.isParameterEmpty()) {
                PrepareRequest prepReq = new PrepareRequest().setStatement(oracleQuery.query());
                PrepareResult prepRes = serviceHandle.prepare(prepReq);
                QueryRequest queryRequest = new QueryRequest().setPreparedStatement(prepRes);
                do {
                    QueryResult queryResult = serviceHandle.query(queryRequest);
                    List<MapValue> results = queryResult.getResults();
                    System.out.println(results);
                } while (!queryRequest.isDone());

            } else {
                System.out.println("has hasParameter");
            }
        }
        return entities.stream();
    }

    private List<DocumentEntity> getIds(OracleQuery oracleQuery) {
        List<DocumentEntity> entities = new ArrayList<>();
        for (String id : oracleQuery.ids()) {
            GetRequest getRequest = new GetRequest();
            getRequest.setKey(new MapValue().put(ID_FIELD, id));
            getRequest.setTableName(name());
            GetResult getResult = serviceHandle.get(getRequest);
            if (getResult != null && getResult.getValue() != null) {
                String json = getResult.getValue().toJson(OPTIONS);
                InputStream stream = new ByteArrayInputStream(json.getBytes(UTF_8));
                JsonReader jsonReader = Json.createReader(stream);
                JsonObject readObject = jsonReader.readObject();
                JsonValue content = readObject.get(JSON_FIELD);
                Map<String, Object> entity = jsonB.fromJson(content.toString(), Map.class);
                List<Document> documents = Documents.of(entity);
                String entityName = Optional.ofNullable(entity.get(ENTITY))
                        .map(Object::toString)
                        .orElseThrow(() -> new OracleDBException("The _entity is required in the entity"));
                DocumentEntity documentEntity = DocumentEntity.of(entityName);
                documentEntity.addAll(documents);
                documentEntity.remove(ENTITY);
                entities.add(documentEntity);
            }
        }

        return entities;
    }

    @Override
    public long count(String documentCollection) {
        return 0;
    }

    @Override
    public void close() {

    }
}
