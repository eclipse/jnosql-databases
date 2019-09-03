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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.column.ColumnEntity;

import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * The Diana wrapper to {@link com.datastax.driver.core.PreparedStatement}
 */
public class CassandraPrepareStatment {

    private final com.datastax.driver.core.PreparedStatement prepare;

    private final Executor executor;

    private final Session session;

    private BoundStatement boundStatement;

    CassandraPrepareStatment(com.datastax.driver.core.PreparedStatement prepare, Executor executor, Session session) {
        this.prepare = prepare;
        this.executor = executor;
        this.session = session;
    }

    public Stream<ColumnEntity> executeQuery() {
        loadBoundStatment();
        ResultSet resultSet = session.execute(boundStatement);
        return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
    }

    /**
     * Executes and call the callback with the result
     *
     * @param consumer the callback
     * @throws ExecuteAsyncQueryException when has async error
     * @throws NullPointerException       when consumer is null
     */
    public void executeQueryAsync(Consumer<Stream<ColumnEntity>> consumer) throws ExecuteAsyncQueryException, NullPointerException {
        loadBoundStatment();
        ResultSetFuture resultSet = session.executeAsync(boundStatement);
        CassandraReturnQueryAsync executeAsync = new CassandraReturnQueryAsync(resultSet, consumer);
        resultSet.addListener(executeAsync, executor);
    }

    /**
     * Bind
     *
     * @param values the values
     * @return this instance
     */
    public CassandraPrepareStatment bind(Object... values) {
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
