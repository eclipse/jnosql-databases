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
package org.eclipse.jnosql.databases.arangodb.mapping;



import org.eclipse.jnosql.mapping.document.JNoSQLDocumentTemplate;

import java.util.Map;
import java.util.stream.Stream;
/**
 * A template specialization for ArangoDB that includes capabilities for executing ArangoDB query language, AQL.
 * This template extends {@link JNoSQLDocumentTemplate} and provides methods for executing AQL queries with various
 * options.
 */
public interface ArangoDBTemplate extends JNoSQLDocumentTemplate {

    /**
     * Executes an ArangoDB query using the ArangoDB Query Language (AQL).
     *
     * <p>Example query: {@code FOR u IN users FILTER u.status == @status RETURN u}</p>
     *
     * <p>The conversion from the query result to entities will happen at the Eclipse JNoSQL side.
     * It will utilize and consider all the annotations supported by Eclipse JNoSQL.</p>
     *
     * @param <T>    the entity class
     * @param query  the AQL query
     * @param params the named parameters for the query
     * @return a {@link Stream} of entities representing the query result
     * @throws NullPointerException when either the query or params are null
     */
    <T> Stream<T> aql(String query, Map<String, Object> params);

    /**
     * Executes an ArangoDB query using the ArangoDB Query Language (AQL).
     *
     * <p>Example query: {@code FOR u IN users FILTER u.status == @status RETURN u}</p>
     *
     * <p>The serialization of the query result will happen at the ArangoDB side using
     * {@link com.arangodb.ArangoDatabase#query(String, Class)}. This serialization does not have any converter support
     * at the mapper side,
     * thus it will ignore any annotations that Eclipse JNoSQL has.</p>
     *
     * @param query  the AQL query
     * @param params the named parameters for the query
     * @param type   the type of the result
     * @param <T>    the type
     * @return a {@link Stream} of the specified type representing the query result
     * @throws NullPointerException when either the query or params are null
     */
    <T> Stream<T> aql(String query, Map<String, Object> params, Class<T> type);

    /**
     * Executes an ArangoDB query using the ArangoDB Query Language (AQL) with an empty parameter map.
     *
     * <p>Example query: {@code FOR u IN users FILTER u.status == @status RETURN u}</p>
     *
     * <p>The serialization of the query result will happen at the ArangoDB side using
     * {@link com.arangodb.ArangoDatabase#query(String, Class)}. This serialization does not have any converter support
     * at the mapper side,
     * thus it will ignore any annotations that Eclipse JNoSQL has.</p>
     *
     * @param query the AQL query
     * @param type  the type of the result
     * @param <T>   the type
     * @return a {@link Stream} of the specified type representing the query result
     * @throws NullPointerException when either the query or type are null
     */
    <T> Stream<T> aql(String query, Class<T> type);
}
