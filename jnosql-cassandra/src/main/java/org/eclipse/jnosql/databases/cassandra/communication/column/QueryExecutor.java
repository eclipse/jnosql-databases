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
package org.eclipse.jnosql.databases.cassandra.communication.column;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import org.eclipse.jnosql.communication.column.ColumnEntity;
import org.eclipse.jnosql.communication.column.ColumnQuery;

import java.util.stream.Stream;

interface QueryExecutor {

    static QueryExecutor of(ColumnQuery query) {
        if (CassandraQuery.class.isInstance(query)) {
            return QueryExecutorType.PAGING_STATE;
        }
        return QueryExecutorType.DEFAULT;
    }

    Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnManager manager);

    Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, ConsistencyLevel level,
                                 DefaultCassandraColumnManager manager);

}