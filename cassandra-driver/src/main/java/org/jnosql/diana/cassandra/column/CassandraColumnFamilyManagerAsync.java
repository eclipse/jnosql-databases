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
package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

/**
 * The implementation of {@link ColumnFamilyManagerAsync} whose implements all methods and also has support to CQL and
 * consistencyLevel.
 * <p>{@link CassandraColumnFamilyManagerAsync#save(ColumnEntity, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#save(ColumnEntity, Duration, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#delete(ColumnDeleteQuery, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#delete(ColumnDeleteQuery, ConsistencyLevel, Consumer)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#cql(String, Consumer)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#find(ColumnQuery, ConsistencyLevel, Consumer)}</p>
 */
public class CassandraColumnFamilyManagerAsync implements ColumnFamilyManagerAsync {

    private final Session session;

    private final Executor executor;

    private final String keyspace;

    CassandraColumnFamilyManagerAsync(Session session, Executor executor, String keyspace) {
        this.session = session;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    @Override
    public void save(ColumnEntity entity) {
        Insert insert = QueryUtils.insert(entity, keyspace);
        session.executeAsync(insert);
    }

    /**
     * Save the entity with ConsistencyLevel
     *
     * @param entity the entity
     * @param level  {@link ConsistencyLevel}
     */
    public void save(ColumnEntity entity, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException{
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(insert);
    }

    /**
     * Save the entity with ConsistencyLevel
     *
     * @param callBack the callback
     * @param entity   the entity
     * @param level    {@link ConsistencyLevel}
     */
    public void save(ColumnEntity entity, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException{
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(callBack, "consumer is required");

        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }

    @Override
    public void save(ColumnEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
    }

    /**
     * Saves the entity with ConsistencyLevel
     *
     * @param entity the entity
     * @param ttl    the ttl
     * @param level  {@link ConsistencyLevel}
     * @throws ExecuteAsyncQueryException
     * @throws UnsupportedOperationException
     */
    public void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
    }

    /**
     * Saves the entity with ConsistencyLevel
     *
     * @param callBack the callBack
     * @param entity   the entity
     * @param ttl      the ttl
     * @param level    {@link ConsistencyLevel}
     * @throws ExecuteAsyncQueryException
     * @throws UnsupportedOperationException
     */
    public void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Objects.requireNonNull(callBack, "consumer is required");
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }

    @Override
    public void save(ColumnEntity entity, Consumer<ColumnEntity> consumer) {
        Insert insert = QueryUtils.insert(entity, keyspace);
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> consumer.accept(entity), executor);
    }

    @Override
    public void save(ColumnEntity entity, Duration ttl, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }


    @Override
    public void update(ColumnEntity entity) {
        save(entity);
    }

    @Override
    public void update(ColumnEntity entity, Consumer<ColumnEntity> consumer) {
        save(entity, consumer);
    }

    @Override
    public void delete(ColumnDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.executeAsync(delete);
    }

    /**
     * Deletes an entity with consistency level
     *
     * @param query the query
     * @param level {@link ConsistencyLevel}
     * @throws NullPointerException when both query or level are null
     */
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(level, "level is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(delete);
    }

    /**
     * Deletes an entity with consistencyLeel and consumter
     *
     * @param query    the query
     * @param consumer the callback
     * @param level    {@link ConsistencyLevel}
     */
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level, Consumer<Void> consumer) {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(level, "level is required");
        Objects.requireNonNull(consumer, "consumer is required");

        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSetFuture = session.executeAsync(delete);
        resultSetFuture.addListener(() -> consumer.accept(null), executor);
    }

    @Override
    public void delete(ColumnDeleteQuery query, Consumer<Void> consumer) {
        Objects.requireNonNull(query, "query is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        ResultSetFuture resultSetFuture = session.executeAsync(delete);
        resultSetFuture.addListener(() -> consumer.accept(null), executor);
    }

    @Override
    public void find(ColumnQuery query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(consumer, "consumer is required");

        BuiltStatement select = QueryUtils.add(query, keyspace);
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    /**
     * Find async with ConsistencyLevel
     *
     * @param query    the query
     * @param level    {@link ConsistencyLevel}
     * @param consumer the callBack
     * @throws ExecuteAsyncQueryException a thread exception
     * @throws NullPointerException       when any arguments are null
     */
    public void find(ColumnQuery query, ConsistencyLevel level, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {
        BuiltStatement select = QueryUtils.add(query, keyspace);
        select.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    /**
     * Executes CQL
     *
     * @param query    the query
     * @param consumer the callback
     * @throws ExecuteAsyncQueryException a thread exception
     */
    public void cql(String query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(consumer, "consumer is required");
        ResultSetFuture resultSet = session.executeAsync(query);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    /**
     * Executes statement
     *
     * @param statement the query
     * @param consumer  the callback
     * @throws ExecuteAsyncQueryException a thread exception
     * @throws NullPointerException       when either statment and callback is null
     */
    public void execute(Statement statement, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {

        Objects.requireNonNull(statement, "statement is required");
        Objects.requireNonNull(consumer, "consumer is required");

        ResultSetFuture resultSet = session.executeAsync(statement);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void close() {
        session.close();
    }


}
