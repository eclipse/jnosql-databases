/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.databases.cassandra.mapping;


import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import org.eclipse.jnosql.communication.semistructured.DeleteQuery;
import org.eclipse.jnosql.communication.semistructured.SelectQuery;
import org.eclipse.jnosql.mapping.column.ColumnTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A Cassandra extension of {@link ColumnTemplate}
 */
public interface CassandraTemplate extends ColumnTemplate {

    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param <T>    type
     * @param entity the entity
     * @param level  the {@link ConsistencyLevel}
     * @return the entity saved
     * @throws NullPointerException when both entity or level are null
     */

    <T> T save(T entity, ConsistencyLevel level);


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param <T>      type
     * @param entities the entities
     * @param ttl      the ttl
     * @param level    the level
     * @return the entity saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    <T> Iterable<T> save(Iterable<T> entities, Duration ttl, ConsistencyLevel level);

    /**
     * Saves a ColumnEntity with a defined ConsistencyLevel
     *
     * @param <T>      type
     * @param entities the entities
     * @param level    the {@link ConsistencyLevel}
     * @return the entity saved
     * @throws NullPointerException when both entity or level are null
     */

    <T> Iterable<T> save(Iterable<T> entities, ConsistencyLevel level);


    /**
     * Saves an entity using {@link ConsistencyLevel}
     *
     * @param <T>    type
     * @param entity the entity
     * @param ttl    the ttl
     * @param level  the level
     * @return the entity saved
     * @throws NullPointerException when either entity or ttl or level are null
     */
    <T> T save(T entity, Duration ttl, ConsistencyLevel level);


    /**
     * Deletes an information using {@link ConsistencyLevel}
     *
     * @param query the query
     * @param level the level
     * @throws NullPointerException when either query or level are null
     */
    void delete(DeleteQuery query, ConsistencyLevel level);

    /**
     * Finds using a consistency level
     *
     * @param <T>   type
     * @param query the query
     * @param level the consistency level
     * @return the query using a consistency level
     */
    <T> Stream<T> find(SelectQuery query, ConsistencyLevel level);

    /**
     * Executes CQL
     *
     * @param <T>   type
     * @param query the Cassandra query language
     * @return the result of this query
     * @throws NullPointerException when query is null
     */
    <T> Stream<T> cql(String query);

    /**
     * Executes CQL using the provided named values.
     * E.g.: "SELECT * FROM users WHERE id = :i", Map.&#60;String, Object&#62;of("i", 1)"
     *
     * @param <T>    type
     * @param query  the Cassandra query language
     * @param values values required for the execution of {@code query}
     * @return the result of this query
     * @throws NullPointerException when query is null
     */
    <T> Stream<T> cql(String query, Map<String, Object> values);

    /**
     * Executes CQL
     *
     * @param <T>    type
     * @param query  the Cassandra query language
     * @param params the params
     * @return the result of this query
     * @throws NullPointerException when query is null
     */
    <T> Stream<T> cql(String query, Object... params);

    /**
     * Executes a statement
     *
     * @param <T>       type
     * @param statement the statement
     * @return the result of this query
     * @throws NullPointerException when statement is null
     */
    <T> Stream<T> execute(SimpleStatement statement);

}
