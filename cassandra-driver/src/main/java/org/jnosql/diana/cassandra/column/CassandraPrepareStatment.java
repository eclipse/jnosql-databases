/*
 * Copyright 2017 Eclipse Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jnosql.diana.cassandra.column;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnEntity;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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

    public List<ColumnEntity> executeQuery() {
        loadBoundStatment();
        ResultSet resultSet = session.execute(boundStatement);
        return resultSet.all().stream().map(row -> CassandraConverter.toDocumentEntity(row))
                .collect(Collectors.toList());
    }


    /**
     * Executes and call the callback with the result
     *
     * @param consumer the callback
     * @throws ExecuteAsyncQueryException
     * @throws NullPointerException       when consumer is null
     */
    public void executeQueryAsync(Consumer<List<ColumnEntity>> consumer) throws ExecuteAsyncQueryException, NullPointerException {
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
