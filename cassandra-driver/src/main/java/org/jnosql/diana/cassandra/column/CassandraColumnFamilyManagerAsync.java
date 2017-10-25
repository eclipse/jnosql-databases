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
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnFamilyManagerAsync;
import org.jnosql.diana.api.column.ColumnQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The implementation of {@link ColumnFamilyManagerAsync} whose implements all methods and also has support to CQL and
 * consistencyLevel.
 * <p>{@link CassandraColumnFamilyManagerAsync#save(ColumnEntity, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#save(ColumnEntity, Duration, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#delete(ColumnDeleteQuery, ConsistencyLevel)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#delete(ColumnDeleteQuery, ConsistencyLevel, Consumer)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#cql(String, Consumer)}</p>
 * <p>{@link CassandraColumnFamilyManagerAsync#select(ColumnQuery, ConsistencyLevel, Consumer)}</p>
 */
public interface CassandraColumnFamilyManagerAsync extends ColumnFamilyManagerAsync {


    /**
     * Save the entity with ConsistencyLevel
     *
     * @param entity the entity
     * @param level  {@link ConsistencyLevel}
     */
    void save(ColumnEntity entity, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException;

    /**
     * Save the entity with ConsistencyLevel
     *
     * @param entities the entities
     * @param level    {@link ConsistencyLevel}
     */
    void save(Iterable<ColumnEntity> entities, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException;

    /**
     * Save the entity with ConsistencyLevel
     *
     * @param callBack the callback
     * @param entity   the entity
     * @param level    {@link ConsistencyLevel}
     */
    void save(ColumnEntity entity, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException;



    /**
     * Saves the entity with ConsistencyLevel
     *
     * @param entity the entity
     * @param ttl    the ttl
     * @param level  {@link ConsistencyLevel}
     * @throws ExecuteAsyncQueryException
     * @throws UnsupportedOperationException
     */
    void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException;

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
    void save(ColumnEntity entity, Duration ttl, ConsistencyLevel level, Consumer<ColumnEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException;

    /**
     * Saves the entity with ConsistencyLevel
     *
     * @param entities the entities
     * @param ttl      the ttl
     * @param level    {@link ConsistencyLevel}
     * @throws ExecuteAsyncQueryException
     * @throws UnsupportedOperationException
     */
    void save(Iterable<ColumnEntity> entities, Duration ttl, ConsistencyLevel level) throws ExecuteAsyncQueryException, UnsupportedOperationException;


    /**
     * Deletes an entity with consistency level
     *
     * @param query the query
     * @param level {@link ConsistencyLevel}
     * @throws NullPointerException when both query or level are null
     */
    void delete(ColumnDeleteQuery query, ConsistencyLevel level) throws NullPointerException;

    /**
     * Deletes an entity with consistencyLeel and consumter
     *
     * @param query    the query
     * @param consumer the callback
     * @param level    {@link ConsistencyLevel}
     */
    void delete(ColumnDeleteQuery query, ConsistencyLevel level, Consumer<Void> consumer);


    /**
     * Find async with ConsistencyLevel
     *
     * @param query    the query
     * @param level    {@link ConsistencyLevel}
     * @param consumer the callBack
     * @throws ExecuteAsyncQueryException a thread exception
     * @throws NullPointerException       when any arguments are null
     */
    void select(ColumnQuery query, ConsistencyLevel level, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException;

    /**
     * Executes CQL
     *
     * @param query    the query
     * @param consumer the callback
     * @throws ExecuteAsyncQueryException a thread exception
     */
    void cql(String query, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException;

    /**
     *Executes CQL using the provided named values.
     * E.g.: "SELECT * FROM users WHERE id = :i", Map.<String, Object>of("i", 1)"
     *
     *  @param query    the query
     *                  @param values the params values
     * @param consumer the callback
     * @throws ExecuteAsyncQueryException a thread exception
     */
    void cql(String query, Map<String, Object> values, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException;

    /**
     * Executes statement
     *
     * @param statement the query
     * @param consumer  the callback
     * @throws ExecuteAsyncQueryException a thread exception
     * @throws NullPointerException       when either statment and callback is null
     */
    void execute(Statement statement, Consumer<List<ColumnEntity>> consumer)
            throws ExecuteAsyncQueryException, NullPointerException;

    /**
     * Executes an query and uses as {@link CassandraPrepareStatment}
     *
     * @param query the query
     * @return the CassandraPrepareStatment instance
     * @throws NullPointerException when query is null
     */
    CassandraPrepareStatment nativeQueryPrepare(String query) throws NullPointerException;

}
