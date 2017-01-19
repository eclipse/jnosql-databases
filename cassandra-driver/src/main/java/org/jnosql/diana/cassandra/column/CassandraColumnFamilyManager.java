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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class CassandraColumnFamilyManager implements ColumnFamilyManager {


    private final Session session;

    private final Executor executor;

    private final String keyspace;

    CassandraColumnFamilyManager(Session session, Executor executor, String keyspace) {
        this.session = session;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    @Override
    public ColumnEntity save(ColumnEntity entity) {
        Insert insert = QueryUtils.insert(entity, keyspace);
        session.execute(insert);
        return entity;
    }

    public ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) throws NullPointerException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.execute(insert);
        return entity;
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, Duration ttl) throws NullPointerException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }

    public ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        Insert insert = QueryUtils.insert(entity, keyspace);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }


    @Override
    public void delete(ColumnQuery query) {
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.execute(delete);
    }

    public void delete(ColumnQuery query, ConsistencyLevel level) {
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.execute(delete);
    }

    @Override
    public List<ColumnEntity> find(ColumnQuery query) {
        BuiltStatement select = QueryUtils.add(query, keyspace);
        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    public List<ColumnEntity> find(ColumnQuery query, ConsistencyLevel level) {
        BuiltStatement select = QueryUtils.add(query, keyspace);
        select.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    public List<ColumnEntity> cql(String query) {
        ResultSet resultSet = session.execute(query);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    public void cql(String query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException {
        ResultSetFuture resultSet = session.executeAsync(query);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    public CassandraPrepareStatment nativeQueryPrepare(String query) {
        com.datastax.driver.core.PreparedStatement prepare = session.prepare(query);
        return new CassandraPrepareStatment(prepare, executor, session);
    }

    @Override
    public void close() {
        session.close();
    }

    Session getSession() {
        return session;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraColumnFamilyManager{");
        sb.append("session=").append(session);
        sb.append('}');
        return sb.toString();
    }
}
