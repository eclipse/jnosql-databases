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

package org.eclipse.jnosql.diana.cassandra.column;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.Insert;
import jakarta.nosql.column.ColumnDeleteQuery;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;


class DefaultCassandraColumnFamilyManager implements CassandraColumnFamilyManager {

    private final CqlSession session;

    private final Executor executor;

    private final String keyspace;

    DefaultCassandraColumnFamilyManager(CqlSession session, Executor executor, String keyspace) {
        this.session = session;
        this.executor = executor;
        this.keyspace = keyspace;
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity) {
        requireNonNull(entity, "entity is required");
        final Insert insert = QueryUtils.insert(entity, keyspace, session, null);
        session.execute(insert.build());
        return entity;
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity, Duration duration) {
        requireNonNull(entity, "entity is required");
        requireNonNull(duration, "duration is required");
        final Insert insert = QueryUtils.insert(entity, keyspace, session, duration);
        session.execute(insert.build());
        return entity;
    }

    @Override
    public ColumnEntity update(ColumnEntity entity) {
        return insert(entity);
    }

    @Override
    public Iterable<ColumnEntity> update(Iterable<ColumnEntity> entities) {
        return insert(entities);
    }


    @Override
    public Iterable<ColumnEntity> insert(Iterable<ColumnEntity> entities) {
        requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<ColumnEntity> insert(Iterable<ColumnEntity> entities, Duration duration) {
        requireNonNull(entities, "entities is required");
        requireNonNull(duration, "entities is duration");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(d -> insert(d, duration))
                .collect(Collectors.toList());
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) {
        requireNonNull(entity, "entities is required");
        requireNonNull(level, "level is required");

        final Insert insert = QueryUtils.insert(entity, keyspace, session, null);
        session.execute(insert.build().setConsistencyLevel(level));
        return entity;
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) {
        requireNonNull(entity, "entity is required");
        requireNonNull(level, "level is required");
        requireNonNull(ttl, "ttl is required");

        final Insert insert = QueryUtils.insert(entity, keyspace, session, ttl);
        session.execute(insert.build().setConsistencyLevel(level));
        return entity;
    }

    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, ConsistencyLevel level) {
        requireNonNull(entities, "entities is required");
        requireNonNull(level, "level is required");
        return StreamSupport.stream(entities.spliterator(), false).map(c -> this.save(c, level))
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) {
        requireNonNull(entities, "entities is required");
        requireNonNull(level, "level is required");
        requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false).map(c -> this.save(c, ttl, level))
                .collect(Collectors.toList());
    }

    @Override
    public Stream<ColumnEntity> select(ColumnQuery query) {
        requireNonNull(query, "query is required");
        QueryExecutor executor = QueryExecutor.of(query);
        return executor.execute(keyspace, query, this);
    }

    @Override
    public Stream<ColumnEntity> select(ColumnQuery query, ConsistencyLevel level) throws NullPointerException {
        requireNonNull(query, "query is required");
        QueryExecutor executor = QueryExecutor.of(query);
        return executor.execute(keyspace, query, level, this);
    }

    @Override
    public long count(String columnFamily) {
        requireNonNull(columnFamily, "columnFamily is required");
        final ResultSet execute = session.execute(QueryBuilder.selectFrom(keyspace, columnFamily).countAll().build());
        return execute.one().getLong(0);
    }


    @Override
    public void close() {
        session.close();
    }

    @Override
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) {
        requireNonNull(query, "query is required");
        requireNonNull(level, "level is required");
        final Delete delete = DeleteQueryConverter.delete(query, keyspace);
        final SimpleStatement build = delete.build();
        final SimpleStatement simpleStatement = build.setConsistencyLevel(level);
        session.execute(simpleStatement);
    }

    @Override
    public void delete(ColumnDeleteQuery query) {
        requireNonNull(query, "query is required");
        final Delete delete = DeleteQueryConverter.delete(query, keyspace);
        session.execute(delete.build());
    }


    @Override
    public Stream<ColumnEntity> cql(String query) {
        requireNonNull(query, "query is required");
        final ResultSet resultSet = session.execute(query);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
    }

    @Override
    public Stream<ColumnEntity> cql(String query, Map<String, Object> values) {
        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");
        final PreparedStatement prepare = session.prepare(query);
        BoundStatement statement = prepare.bind();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            final TypeCodec<Object> codec = CodecRegistry.DEFAULT.codecFor((Class<Object>) entry.getValue().getClass());
            statement = statement.set(entry.getKey(), entry.getValue(), codec);
        }
        final ResultSet resultSet = session.execute(statement);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
    }

    @Override
    public Stream<ColumnEntity> execute(SimpleStatement statement) {
        requireNonNull(statement, "statement is required");
        final ResultSet resultSet = session.execute(statement);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
    }

    @Override
    public CassandraPreparedStatement nativeQueryPrepare(String query) {
        requireNonNull(query, "query is required");
        final PreparedStatement prepare = session.prepare(query);
        return new CassandraPreparedStatement(prepare, session);
    }


    CqlSession getSession() {
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

