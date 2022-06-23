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
package org.eclipse.jnosql.communication.arangodb.document;

import jakarta.nosql.document.DocumentCollectionManager;
import jakarta.nosql.document.DocumentEntity;

import java.util.Map;
import java.util.stream.Stream;

/**
 * The ArangoDB implementation of {@link DocumentCollectionManager} it does not support to TTL methods:
 * <p>{@link DocumentCollectionManager#insert(DocumentEntity)}</p>
 */
public interface ArangoDBDocumentCollectionManager extends DocumentCollectionManager {

    /**
     * Executes ArangoDB query language, AQL.
     * <p>FOR u IN users FILTER u.status == @status RETURN u </p>
     *
     * @param query  the query
     * @param values the named queries
     * @return the query result
     * @throws NullPointerException when either query or values are null
     */
    Stream<DocumentEntity> aql(String query, Map<String, Object> values);

    /**
     * Executes ArangoDB query language, AQL.
     * <p>FOR u IN users FILTER u.status == @status RETURN u </p>
     *
     * @param query     the query
     * @param values    named query
     * @param typeClass The type of the result
     * @param <T>       the type
     * @return the query result
     * @throws NullPointerException when either query or values are null
     */
    <T> Stream<T> aql(String query, Map<String, Object> values, Class<T> typeClass);

    /**
     * Executes ArangoDB query language, AQL.
     * <p>FOR u IN users FILTER u.status == @status RETURN u </p>
     *
     * @param query     the query
     * @param typeClass The type of the result
     * @param <T>       the type
     * @return the query result
     * @throws NullPointerException when either query or values are null
     */
    <T> Stream<T> aql(String query, Class<T> typeClass);
}
