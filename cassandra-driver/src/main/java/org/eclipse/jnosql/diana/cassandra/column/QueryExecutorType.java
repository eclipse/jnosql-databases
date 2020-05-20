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

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;

import java.util.Objects;
import java.util.stream.Stream;

enum QueryExecutorType implements QueryExecutor {

    DEFAULT {
        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnFamilyManager manager) {
            return execute(keyspace, query, null, manager);
        }

        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, ConsistencyLevel level,
                                            DefaultCassandraColumnFamilyManager manager) {
            SimpleStatement select = QueryUtils.select(query, keyspace).build();
            if (Objects.nonNull(level)) {
                select = select.setConsistencyLevel(level);
            }
            ResultSet resultSet = manager.getSession().execute(select);
            return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
        }
    }
}
