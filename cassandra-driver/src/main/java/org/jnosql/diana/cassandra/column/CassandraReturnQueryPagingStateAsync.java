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

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.column.ColumnEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

final class CassandraReturnQueryPagingStateAsync implements Runnable {

    private final ResultSetFuture resultSet;

    private final Consumer<List<ColumnEntity>> consumer;

    private final CassandraQuery query;


    CassandraReturnQueryPagingStateAsync(ResultSetFuture resultSet, Consumer<List<ColumnEntity>> consumer, CassandraQuery query) {
        this.resultSet = resultSet;
        this.consumer = consumer;
        this.query = query;
    }

    @Override
    public void run() {
        try {
            ResultSet resultSet = this.resultSet.get();

            synchronized (query) {
                PagingState pagingState = resultSet.getExecutionInfo().getPagingState();
                query.setPagingState(pagingState);
            }
            List<ColumnEntity> entities = new ArrayList<>();
            for (Row row : resultSet) {
                entities.add(CassandraConverter.toDocumentEntity(row));
                if (resultSet.getAvailableWithoutFetching() == 0) {
                    query.setExhausted(resultSet.isExhausted());
                    break;
                }
            }
            consumer.accept(entities);
        } catch (InterruptedException | ExecutionException e) {
            throw new ExecuteAsyncQueryException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraReturnQueryPagingStateAsync{");
        sb.append("resultSet=").append(resultSet);
        sb.append(", consumer=").append(consumer);
        sb.append(", query=").append(query);
        sb.append('}');
        return sb.toString();
    }
}
