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
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import jakarta.nosql.column.ColumnDeleteQuery;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;

import java.time.Duration;
import java.util.Map;
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
        final RegularInsert insert = QueryUtils.insert(entity, keyspace, session, null);
        session.execute(insert.build());
        return entity;
    }

    @Override
    public ColumnEntity insert(ColumnEntity entity, Duration duration) {
        requireNonNull(entity, "entity is required");
        requireNonNull(duration, "duration is required");
        final RegularInsert insert = QueryUtils.insert(entity, keyspace, session, duration);
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
    public void delete(ColumnDeleteQuery columnDeleteQuery) {

    }

    @Override
    public Stream<ColumnEntity> select(ColumnQuery query) {
        requireNonNull(query, "query is required");
        QueryExecutor executor = QueryExecutor.of(query);
        return executor.execute(keyspace, query, this);
    }

    @Override
    public long count(String s) {
        return 0;
    }


    @Override
    public void close() {
        session.close();
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

    @Override
    public ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) throws NullPointerException {
        return null;
    }

    @Override
    public ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        return null;
    }

    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws NullPointerException {
        return null;
    }

    @Override
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        return null;
    }

    @Override
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException {

    }

    @Override
    public Stream<ColumnEntity> select(ColumnQuery query, ConsistencyLevel level) throws NullPointerException {
        return null;
    }

    @Override
    public Stream<ColumnEntity> cql(String query) throws NullPointerException {
        return null;
    }

    @Override
    public Stream<ColumnEntity> cql(String query, Map<String, Object> values) throws NullPointerException {
        return null;
    }

    @Override
    public Stream<ColumnEntity> execute(SimpleStatement statement) throws NullPointerException {
        return null;
    }

    @Override
    public CassandraPreparedStatement nativeQueryPrepare(String query) throws NullPointerException {
        return null;
    }
}

