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
package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

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

    public void save(ColumnEntity entity, ConsistencyLevel level) {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(insert);
    }

    @Override
    public void save(ColumnEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
    }

    public void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.executeAsync(insert);
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
    public void delete(ColumnQuery query) {
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.executeAsync(delete);
    }

    public void delete(ColumnQuery query, ConsistencyLevel level) {
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.executeAsync(delete);
    }

    @Override
    public void delete(ColumnQuery query, Consumer<Void> consumer) {
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        ResultSetFuture resultSetFuture = session.executeAsync(delete);
        resultSetFuture.addListener(() -> consumer.accept(null), executor);
    }

    @Override
    public void find(ColumnQuery query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        BuiltStatement select = QueryUtils.add(query, keyspace);
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    public void findAsync(ColumnQuery query, ConsistencyLevel level, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        BuiltStatement select = QueryUtils.add(query, keyspace);
        select.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSetFuture resultSet = session.executeAsync(select);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    @Override
    public void close() {
        session.close();
    }


}
