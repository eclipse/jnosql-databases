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
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;

/**
 * The default implementation of {@link CassandraColumnFamilyManagerAsync}
 */
class DefaultCassandraColumnFamilyManagerAsync implements CassandraColumnFamilyManagerAsync {

    private final Session session;

    private final Executor executor;

    private final String keyspace;

    DefaultCassandraColumnFamilyManagerAsync(Session session, Executor executor, String keyspace) {
        this.session = session;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    @Override
    public void insert(ColumnEntity entity) {
        requireNonNull(entity, "entity is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        session.executeAsync(insert);
    }

    @Override
    public void save(ColumnEntity entity, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(level, "level is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(insert);
    }

    @Override
    public void save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entities, "entities is required");
        requireNonNull(level, "level is required");

        StreamSupport.stream(entities.spliterator(), false).forEach(e -> this.save(e, level));
    }

    @Override
    public void save(ColumnEntity entity, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "consumer is required");
        requireNonNull(level, "ConsistencyLevel is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(level);
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }

    @Override
    public void insert(ColumnEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
    }

    @Override
    public void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        requireNonNull(level, "level is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
    }

    @Override
    public void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "consumer is required");
        requireNonNull(level, "level is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(level);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }

    @Override
    public void save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entities, "entities is required");
        StreamSupport.stream(entities.spliterator(), false).forEach(e -> this.save(e, ttl, level));
    }

    @Override
    public void insert(ColumnEntity entity, Consumer<ColumnEntity> consumer) {
        requireNonNull(entity, "entity is required");
        requireNonNull(consumer, "consumer is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> consumer.accept(entity), executor);
    }

    @Override
    public void insert(ColumnEntity entity, Duration ttl, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        requireNonNull(callBack, "callBack is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        ResultSetFuture resultSetFuture = session.executeAsync(insert);
        resultSetFuture.addListener(() -> callBack.accept(entity), executor);
    }


    @Override
    public void update(ColumnEntity entity) {
        insert(entity);
    }

    @Override
    public void update(ColumnEntity entity, Consumer<ColumnEntity> consumer) {
        insert(entity, consumer);
    }

    @Override
    public void delete(ColumnDeleteQuery query) {
        requireNonNull(query, "query is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.executeAsync(delete);
    }

    @Override
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException {

        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");

        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(delete);
    }

    @Override
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level, Consumer<Void> consumer) {
        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");
        requireNonNull(consumer, "consumer is required");

        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSetFuture = session.executeAsync(delete);
        resultSetFuture.addListener(() -> consumer.accept(null), executor);
    }

    @Override
    public void delete(ColumnDeleteQuery query, Consumer<Void> consumer) {

        requireNonNull(query, "query is required");
        requireNonNull(consumer, "consumer is required");

        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        ResultSetFuture resultSetFuture = session.executeAsync(delete);
        resultSetFuture.addListener(() -> consumer.accept(null), executor);
    }

    @Override
    public void select(ColumnQuery query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(query, "query is required");
        requireNonNull(consumer, "consumer is required");

        BuiltStatement select = QueryUtils.select(query, keyspace);
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void count(String columnFamily, Consumer<Long> callback) {
        requireNonNull(columnFamily, "columnFamily is required");
        requireNonNull(callback, "callback is required");
    }

    @Override
    public void select(ColumnQuery query, ConsistencyLevel level, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {

        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");
        requireNonNull(consumer, "consumer is required");

        BuiltStatement select = QueryUtils.select(query, keyspace);
        select.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void cql(String query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(consumer, "consumer is required");
        ResultSetFuture resultSet = session.executeAsync(query);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void cql(String query, Map<String, Object> values, Consumer<List<ColumnEntity>> consumer) throws ExecuteAsyncQueryException, NullPointerException {

        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");
        requireNonNull(consumer, "consumer is required");

        ResultSetFuture resultSet = session.executeAsync(query, values);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void execute(Statement statement, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException {

        requireNonNull(statement, "statement is required");
        requireNonNull(consumer, "consumer is required");

        ResultSetFuture resultSet = session.executeAsync(statement);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public CassandraPrepareStatment nativeQueryPrepare(String query) throws NullPointerException {
        requireNonNull(query, "query is required");
        com.datastax.driver.core.PreparedStatement prepare = session.prepare(query);
        return new CassandraPrepareStatment(prepare, executor, session);
    }

    @Override
    public void close() {
        session.close();
    }
}
