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
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * The Cassandra implementation of {@link ColumnFamilyManager}, that supports all methods and also supports
 * CQL and ConsistencyLevel.
 * <p>{@link CassandraColumnFamilyManager#select(ColumnQuery, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManager#cql(String)}</p>
 * <p>{@link CassandraColumnFamilyManager#nativeQueryPrepare(String)}</p>
 * <p>{@link CassandraColumnFamilyManager#delete(ColumnDeleteQuery, ConsistencyLevel)}</p>
 */
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
    public ColumnEntity insert(ColumnEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
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
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(ttl, "ttl is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }


    @Override
    public void delete(ColumnDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        session.execute(delete);
    }


    @Override
    public List<ColumnEntity> select(ColumnQuery query) {
        Objects.requireNonNull(query, "query is required");
        BuiltStatement select = QueryUtils.add(query, keyspace);
        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param entity the entity
     * @param level  the {@link ConsistencyLevel}
     * @return the entity saved
     * @throws NullPointerException when both entity or level are null
     */
    public ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(entity, "entity is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.execute(insert);
        return entity;
    }


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param entity the entity
     * @param ttl    the ttl
     * @param level  the level
     * @return the entity saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    public ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(entity, "entity is required");
        Objects.requireNonNull(ttl, "ttl is required");
        Objects.requireNonNull(level, "level is required");
        Insert insert = QueryUtils.insert(entity, keyspace, session);
        insert.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        insert.using(QueryBuilder.ttl((int) ttl.getSeconds()));
        session.execute(insert);
        return entity;
    }
    //

    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param entities the entities
     * @param level    the {@link ConsistencyLevel}
     * @return the entities saved
     * @throws NullPointerException when both entity or level are null
     */
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(entities, "entity is required");

        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.save(e, level))
                .collect(Collectors.toList());
    }


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param entities the entities
     * @param ttl      the ttl
     * @param level    the level
     * @return the entities saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    public Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(entities, "entity is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.save(e, ttl, level))
                .collect(Collectors.toList());
    }
    //

    /**
     * Deletes an information using {@link ConsistencyLevel}
     *
     * @param query the query
     * @param level the level
     * @throws NullPointerException when either query or level are null
     */
    public void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(level, "level is required");
        BuiltStatement delete = QueryUtils.delete(query, keyspace);
        delete.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        session.execute(delete);
    }

    /**
     * Finds using a consistency level
     *
     * @param query the query
     * @param level the consistency level
     * @return the query using a consistency level
     * @throws NullPointerException when either query or level are null
     */
    public List<ColumnEntity> select(ColumnQuery query, ConsistencyLevel level) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(level, "level is required");
        BuiltStatement select = QueryUtils.add(query, keyspace);
        select.setConsistencyLevel(Objects.requireNonNull(level, "ConsistencyLevel is required"));
        ResultSet resultSet = session.execute(select);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    /**
     * Executes CQL
     *
     * @param query the Cassndra query language
     * @return the result of this query
     * @throws NullPointerException when query is null
     */
    public List<ColumnEntity> cql(String query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        ResultSet resultSet = session.execute(query);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    /**
     * Executes a statement
     *
     * @param statement the statement
     * @return the result of this query
     * @throws NullPointerException when statement is null
     */
    public List<ColumnEntity> execute(Statement statement) throws NullPointerException {
        Objects.requireNonNull(statement, "statement is required");
        ResultSet resultSet = session.execute(statement);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }

    /**
     * Executes an query and uses as {@link CassandraPrepareStatment}
     *
     * @param query the query
     * @return the CassandraPrepareStatment instance
     * @throws NullPointerException when query is null
     */
    public CassandraPrepareStatment nativeQueryPrepare(String query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
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
