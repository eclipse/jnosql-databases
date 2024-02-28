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
package org.eclipse.jnosql.databases.oracle.communication;

import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;

import java.util.stream.Stream;

/**
 * A document manager interface for Oracle NoSQL database operations.
 */
public interface OracleNoSQLDocumentManager extends DocumentManager {


    /**
     * Executes an Oracle query using the Oracle Query Language (SQL).
     *
     * @param query the SQL query
     * @return a {@link Stream} of {@link DocumentEntity} representing the query result
     * @throws NullPointerException when the query is null
     */
    Stream<DocumentEntity> sql(String query);

    /**
     * Executes an Oracle query using the Oracle Query Language (SQL) with parameters.
     * <p>Example query: {@code SELECT * FROM users WHERE status = ?}</p>
     *
     * @param query  the SQL query with placeholders for parameters
     * @param params the values to replace the placeholders in the query
     * @return a {@link Stream} of {@link DocumentEntity} representing the query result
     * @throws NullPointerException when either the query or params are null
     */
    Stream<DocumentEntity> sql(String query, Object... params);

}
