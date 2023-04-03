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

package org.eclipse.jnosql.databases.cassandra.communication;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import org.eclipse.jnosql.communication.column.ColumnEntity;

import java.util.stream.Stream;

/**
 * The Diana wrapper to {@link com.datastax.oss.driver.api.core.cql.PreparedStatement}
 */
public class CassandraPreparedStatement {

    private final com.datastax.oss.driver.api.core.cql.PreparedStatement prepare;

    private final CqlSession session;

    private BoundStatement boundStatement;

    CassandraPreparedStatement(com.datastax.oss.driver.api.core.cql.PreparedStatement prepare, CqlSession session) {
        this.prepare = prepare;
        this.session = session;
    }

    public Stream<ColumnEntity> executeQuery() {
        load();
        ResultSet resultSet = session.execute(boundStatement);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
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

    private void load() {
        if (boundStatement == null) {
            boundStatement = prepare.bind();
        }
    }


    @Override
    public String toString() {
        return "CassandraPreparedStatement{" +
                "prepare=" + prepare +
                ", session=" + session +
                ", boundStatement=" + boundStatement +
                '}';
    }
}
