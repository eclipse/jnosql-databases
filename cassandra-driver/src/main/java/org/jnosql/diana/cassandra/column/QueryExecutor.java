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

import com.datastax.driver.core.ConsistencyLevel;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;

import java.util.List;
import java.util.function.Consumer;

interface QueryExecutor {

    static QueryExecutor of(ColumnQuery query) {
        if (CassandraQuery.class.isInstance(query)) {
            return QueryExecutorType.PAGING_STATE;
        }
        return QueryExecutorType.DEFAULT;
    }

    List<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnFamilyManager manager);

    List<ColumnEntity> execute(String keyspace, ColumnQuery query, ConsistencyLevel level,
                               DefaultCassandraColumnFamilyManager manager);

    void execute(String keyspace, ColumnQuery query, ConsistencyLevel level, Consumer<List<ColumnEntity>> consumer,
                 DefaultCassandraColumnFamilyManagerAsync manager);

    void execute(String keyspace, ColumnQuery query, Consumer<List<ColumnEntity>> consumer,
                 DefaultCassandraColumnFamilyManagerAsync manager);
}
