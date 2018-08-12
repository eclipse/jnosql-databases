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
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;


class DefaultCassandraColumnFamilyManager implements CassandraColumnFamilyManager {

    private final Session session;

    private final Executor executor;

    private final String keyspace;

    DefaultCassandraColumnFamilyManager(Session session, Executor executor, String keyspace) {
        this.session = session;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity) {
        requireNonNull(entity, "entity is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        session.execute(insert);
        return entity;
    }

    @Override
    public ColumnEntity update(ColumnEntity entity) throws NullPointerException {
        return insert(entity);
    }


    @Override
    public ColumnEntity insert(ColumnEntity entity, Duration ttl) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }


    @Override
    public void delete(ColumnDeleteQuery query) {
        requireNonNull(query, "query is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.execute(delete);
    }


    @Override
    public List<ColumnEntity> select(ColumnQuery query) {
        requireNonNull(query, "query is required");
        BuiltStatement select = QueryUtils.select(query, keyspace);
        boolean isCassandraQuery = CassandraQuery.class.isInstance(query);
        if (isCassandraQuery) {
            CassandraQuery.class.cast(query).toPatingState().ifPresent(select::setPagingState);
        }
        ResultSet resultSet = session.execute(select);

        if (isCassandraQuery) {
            PagingState pagingState = resultSet.getExecutionInfo().getPagingState();
            CassandraQuery.class.cast(query).setPagingState(pagingState);
        }

        List<ColumnEntity> entities = new ArrayList<>();
        for (Row row : resultSet) {
            entities.add(CassandraConverter.toDocumentEntity(row));
            if (resultSet.getAvailableWithoutFetching() == 0) {
                if (isCassandraQuery) {
                    CassandraQuery.class.cast(query).setPagingState(resultSet.isExhausted());
                }
                break;
            }
        }
        return entities;
    }

    @Override
    public long count(String columnFamily) {
        requireNonNull(columnFamily, "columnFamily is required");
        String cql = QueryUtils.count(columnFamily, keyspace);
        ResultSet resultSet = session.execute(cql);
        Object object = resultSet.one().getObject(0);
        return Number.class.cast(object).longValue();
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        requireNonNull(level, "ConsistencyLevel is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(level);
        session.execute(insert);
        return entity;
    }


    @Override
    public ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(entity, "entity is required");
        requireNonNull(ttl, "ttl is required");
        requireNonNull(level, "level is required");

        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }

    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(entities, "entity is required");
        requireNonNull(level, "level is required");

        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.save(e, level))
                .collect(Collectors.toList());
    }


    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(entities, "entity is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.save(e, ttl, level))
                .collect(Collectors.toList());
    }

    @Override
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        session.execute(delete);
    }

    @Override
    public List<ColumnEntity> select(ColumnQuery query, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");
        BuiltStatement select = QueryUtils.select(query, keyspace);
        select.setConsistencyLevel(requireNonNull(level, "ConsistencyLevel is required"));
        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity)
                .collect(Collectors.toList());
    }

    @Override
    public List<ColumnEntity> cql(String query) throws NullPointerException {
        requireNonNull(query, "query is required");
        ResultSet resultSet = session.execute(query);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity)
                .collect(Collectors.toList());
    }

    @Override
    public List<ColumnEntity> cql(String query, Map<String, Object> values) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");
        ResultSet resultSet = session.execute(query, values);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity)
                .collect(Collectors.toList());
    }

    @Override
    public List<ColumnEntity> execute(Statement statement) throws NullPointerException {
        requireNonNull(statement, "statement is required");
        ResultSet resultSet = session.execute(statement);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity)
                .collect(Collectors.toList());
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

