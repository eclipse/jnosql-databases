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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import jakarta.nosql.column.ColumnEntity;

import java.util.concurrent.Executor;
import java.util.stream.Stream;

/**
 * The Diana wrapper to {@link com.datastax.driver.core.PreparedStatement}
 */
public class CassandraPreparedStatement {

    private final com.datastax.oss.driver.api.core.cql.PreparedStatement prepare;

    private final Executor executor;

    private final CqlSession session;

    private BoundStatement boundStatement;

    CassandraPreparedStatement(com.datastax.oss.driver.api.core.cql.PreparedStatement prepare, Executor executor, CqlSession session) {
        this.prepare = prepare;
        this.executor = executor;
        this.session = session;
    }

    public Stream<ColumnEntity> executeQuery() {
        loadBoundStatment();
        ResultSet resultSet = session.execute(boundStatement);
        return null;
    }


    /**
     * Bind
     *
     * @param values the values
     * @return this instance
     */
    public CassandraPreparedStatement bind(Object... values) {
        boundStatement = prepare.bind(values);
        return this;
    }

    private void loadBoundStatment() {
        if (boundStatement == null) {
            boundStatement = prepare.bind();
        }
    }

    public void close() {

    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraPrepareStatment{");
        sb.append("prepare=").append(prepare);
        sb.append(", executor=").append(executor);
        sb.append(", session=").append(session);
        sb.append(", boundStatement=").append(boundStatement);
        sb.append('}');
        return sb.toString();
    }
}
