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


import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.column.ColumnEntity;

import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.stream.Stream;

class CassandraReturnQueryAsync implements Runnable {

    private final ResultSetFuture resultSet;

    private final Consumer<Stream<ColumnEntity>> consumer;


    CassandraReturnQueryAsync(ResultSetFuture resultSet, Consumer<Stream<ColumnEntity>> consumer) {
        this.resultSet = resultSet;
        this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            ResultSet resultSet = this.resultSet.get();
            Stream<ColumnEntity> entities = resultSet.all().stream()
                    .map(CassandraConverter::toDocumentEntity);
            consumer.accept(entities);
        } catch (InterruptedException | ExecutionException e) {
            throw new ExecuteAsyncQueryException(e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CassandraReturnQueryAsync{");
        sb.append("resultSet=").append(resultSet);
        sb.append(", consumer=").append(consumer);
        sb.append('}');
        return sb.toString();
    }
}
