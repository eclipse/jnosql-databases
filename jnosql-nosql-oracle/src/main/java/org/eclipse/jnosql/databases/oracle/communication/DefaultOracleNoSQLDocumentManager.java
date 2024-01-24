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
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.document.Documents;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.ID_FIELD;
import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.JSON_FIELD;

/**
 * The Oracle implementation to {@link OracleNoSQLDocumentManager}
 */
final class DefaultOracleNoSQLDocumentManager implements OracleNoSQLDocumentManager {

    private final Logger LOGGER = Logger.getLogger(DefaultOracleNoSQLDocumentManager.class.getName());
    private static final JsonOptions OPTIONS = new JsonOptions();
    static final String ENTITY = "entity";
    static final String ID = "_id";
    static final String ORACLE_ID = "id";
    private final String table;
    private final NoSQLHandle serviceHandle;

    private final Jsonb jsonB;
    public DefaultOracleNoSQLDocumentManager(String table, NoSQLHandle serviceHandle, Jsonb jsonB) {
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
                .orElseGet(() -> UUID.randomUUID().toString());

        MapValue mapValue = new MapValue().put(ORACLE_ID, id).put(ENTITY, entity.name());
        MapValue contentVal = mapValue.putFromJson("content", jsonB.toJson(entityMap),
                OPTIONS);
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

        var selectBuilder = new DeleteBuilder(query, table);
        var oracleQuery = selectBuilder.get();
        if (oracleQuery.hasIds()) {
            for (String id : oracleQuery.ids()) {
                var delRequest = new DeleteRequest().setKey(new MapValue().put(ORACLE_ID, id))
                        .setTableName(table);
                serviceHandle.delete(delRequest);
            }
        }
        if (!oracleQuery.hasOnlyIds()) {
            LOGGER.finest("Executing delete query at Oracle NoSQL: " +oracleQuery.query());
            var prepReq = new PrepareRequest().setStatement(oracleQuery.query());
            var prepRes = serviceHandle.prepare(prepReq);
            PreparedStatement preparedStatement = prepRes.getPreparedStatement();
            for (int index = 0; index < oracleQuery.params().size(); index++) {
                preparedStatement.setVariable((index + 1), oracleQuery.params().get(index));
            }

            QueryRequest queryRequest = new QueryRequest().setPreparedStatement(prepRes);
            do {
                QueryResult result = serviceHandle.query(queryRequest);
                List<MapValue> results = result.getResults();
                LOGGER.finest("The delete result: " +results);
            } while (!queryRequest.isDone());
        }
    }


    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");
        var selectBuilder = new SelectBuilder(query, table);
        var oracleQuery = selectBuilder.get();
        List<DocumentEntity> entities = new ArrayList<>();

        if (oracleQuery.hasIds()) {
            entities.addAll(getIds(oracleQuery));
        }
        if (!oracleQuery.hasOnlyIds()) {
            LOGGER.finest("Executing Oracle Query: " + oracleQuery.query());

            entities.addAll(executeSQL(oracleQuery.query(), oracleQuery.params()));
        }
        return entities.stream();
    }



    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");

        var prepReq = new PrepareRequest().setStatement("select count(*)  as count from " + name() + " where " +
                ENTITY +" = ?");
        var prepRes = serviceHandle.prepare(prepReq);
        var preparedStatement = prepRes.getPreparedStatement();
        preparedStatement.setVariable(1, new StringValue(documentCollection));
        var queryRequest = new QueryRequest().setPreparedStatement(prepRes);
        QueryResult queryResult = serviceHandle.query(queryRequest);
        List<MapValue> results = queryResult.getResults();
        if(results.size() == 1) {
            return results.get(0).get("count").asLong().getValue();
        }
        return 0;
    }

    private static boolean isNotOracleField(Map.Entry<String, FieldValue> entry) {
        return !entry.getKey().equals(ENTITY) && !entry.getKey().equals(JSON_FIELD) && !entry.getKey().equals(ORACLE_ID);
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
                        .orElseThrow(() -> new OracleNoSQLException("The _entity is required in the entity"));
                DocumentEntity documentEntity = DocumentEntity.of(entityName);
                documentEntity.addAll(documents);
                documentEntity.remove(ENTITY);
                entities.add(documentEntity);
            }
        }

        return entities;
    }



    @Override
    public void close() {
        this.serviceHandle.close();
    }

    @Override
    public Stream<DocumentEntity> sql(String query) {
        Objects.requireNonNull(query, "query is required");

        return executeSQL(query, Collections.emptyList()).stream();
    }

    @Override
    public Stream<DocumentEntity> sql(String query, Object... params) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(params, "params is required");
        List<FieldValue> fields = Arrays.stream(params).map(FieldValueConverter.INSTANCE::of).toList();
        return executeSQL(query, fields).stream();
    }

    @SuppressWarnings("unchecked")
    private List<DocumentEntity> executeSQL(String sql, List<FieldValue> params) {
        List<DocumentEntity> entities = new ArrayList<>();
        var prepReq = new PrepareRequest().setStatement(sql);
        var prepRes = serviceHandle.prepare(prepReq);
        var preparedStatement = prepRes.getPreparedStatement();
        for (int index = 0; index < params.size(); index++) {
            preparedStatement.setVariable((index + 1), params.get(index));
        }

        var queryRequest = new QueryRequest().setPreparedStatement(prepRes);
        do {
            QueryResult queryResult = serviceHandle.query(queryRequest);
            List<MapValue> results = queryResult.getResults();
            for (MapValue result : results) {

                var entity = DocumentEntity.of(result.get(ENTITY).asString().getValue());
                if(result.get(JSON_FIELD) != null){
                    var json = result.get(JSON_FIELD).toJson();
                    entity.addAll(Documents.of(jsonB.fromJson(json, Map.class)));
                }
                for (Map.Entry<String, FieldValue> entry : result) {
                    if (isNotOracleField(entry)) {
                        entity.add(Document.of(entry.getKey(), FieldValueConverter.INSTANCE.of(entry.getValue())));
                    }
                }
                entity.add(Document.of(ID, result.get(ORACLE_ID).asString().getValue()));
                entities.add(entity);
            }
        } while (!queryRequest.isDone());
        return entities;
    }
}
