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
import com.datastax.driver.core.Statement;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManager;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * The Cassandra implementation of {@link ColumnFamilyManager}, that supports all methods and also supports
 * CQL and ConsistencyLevel.
 * <p>{@link CassandraColumnFamilyManager#select(ColumnQuery, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManager#cql(String)}</p>
 * <p>{@link CassandraColumnFamilyManager#nativeQueryPrepare(String)}</p>
 * <p>{@link CassandraColumnFamilyManager#delete(ColumnDeleteQuery, ConsistencyLevel)}</p>
 */
public interface CassandraColumnFamilyManager extends ColumnFamilyManager {


    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param entity the entity
     * @param level  the {@link ConsistencyLevel}
     * @return the entity saved
     * @throws NullPointerException when both entity or level are null
     */
    ColumnEntity save(ColumnEntity entity, ConsistencyLevel level) throws NullPointerException;


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param entity the entity
     * @param ttl    the ttl
     * @param level  the level
     * @return the entity saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    ColumnEntity save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws NullPointerException;

    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param entities the entities
     * @param level    the {@link ConsistencyLevel}
     * @return the entities saved
     * @throws NullPointerException when both entity or level are null
     */
    Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws NullPointerException;


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param entities the entities
     * @param ttl      the ttl
     * @param level    the level
     * @return the entities saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    Iterable<ColumnEntity> save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws NullPointerException;

    /**
     * Deletes an information using {@link ConsistencyLevel}
     *
     * @param query the query
     * @param level the level
     * @throws NullPointerException when either query or level are null
     */
    void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException;

    /**
     * Finds using a consistency level
     *
     * @param query the query
     * @param level the consistency level
     * @return the query using a consistency level
     * @throws NullPointerException when either query or level are null
     */
    List<ColumnEntity> select(ColumnQuery query, ConsistencyLevel level) throws NullPointerException;

    /**
     * Executes CQL
     *
     * @param query the Cassndra query language
     * @return the result of this query
     * @throws NullPointerException when query is null
     */
    List<ColumnEntity> cql(String query) throws NullPointerException;


    /**
     * Executes CQL using the provided named values.
     * E.g.: "SELECT * FROM users WHERE id = :i", Map.<String, Object>of("i", 1)"
     *
     * @param query  the Cassndra query language
     * @param values the names params
     * @return the result of this query
     * @throws NullPointerException when either query or values are null
     */
    List<ColumnEntity> cql(String query, Map<String, Object> values) throws NullPointerException;

    /**
     * Executes a statement
     *
     * @param statement the statement
     * @return the result of this query
     * @throws NullPointerException when statement is null
     */
    List<ColumnEntity> execute(Statement statement) throws NullPointerException;

    /**
     * Executes an query and uses as {@link CassandraPrepareStatment}
     *
     * @param query the query
     * @return the CassandraPrepareStatment instance
     * @throws NullPointerException when query is null
     */
    CassandraPrepareStatment nativeQueryPrepare(String query) throws NullPointerException;
}
