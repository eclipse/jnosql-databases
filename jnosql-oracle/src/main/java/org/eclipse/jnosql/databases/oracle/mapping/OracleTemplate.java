/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.mapping;

import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.mapping.document.JNoSQLDocumentTemplate;

import java.util.stream.Stream;

/**
 * The {@code OracleTemplate} is an interface that extends {@link JNoSQLDocumentTemplate} and
 * provides methods for executing Oracle SQL queries using the Oracle Query Language (SQL).
 * <p>
 * It allows you to interact with the Oracle NoSQL database using SQL queries to retrieve and
 * process data in a more flexible and customizable way.
 * </p>
 *
 * @see JNoSQLDocumentTemplate
 */
public interface OracleTemplate extends JNoSQLDocumentTemplate {

    /**
     * Executes an Oracle query using the Oracle Query Language (SQL).
     *
     * @param query the SQL query
     * @return a {@link Stream} of results representing the query result
     * @throws NullPointerException when the query is null
     *
     * @param <T> the type of objects in the result stream
     */
    <T> Stream<T> sql(String query);

    /**
     * Executes an Oracle query using the Oracle Query Language (SQL) with parameters.
     * <p>Example query: {@code SELECT * FROM users WHERE status = ?}</p>
     *
     * @param query  the SQL query with placeholders for parameters
     * @param params the values to replace the placeholders in the query
     * @return a {@link Stream} of results representing the query result
     * @throws NullPointerException when either the query or params are null
     *
     * @param <T> the type of objects in the result stream
     */
    <T>  Stream<T> sql(String query, Object... params);

}
