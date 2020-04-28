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

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

enum QueryExecutorType implements QueryExecutor {

    PAGING_STATE {
        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnFamilyManager manager) {
            return execute(keyspace, query, null, manager);
        }

        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery q, ConsistencyLevel level,
                                            DefaultCassandraColumnFamilyManager manager) {
            CassandraQuery query = CassandraQuery.class.cast(q);

            if (query.isExhausted()) {
                return Stream.empty();
            }
            BuiltStatement select = QueryUtils.select(query, keyspace);

            if (Objects.nonNull(level)) {
                select.setConsistencyLevel(level);
            }

            query.toPatingState().ifPresent(select::setPagingState);
            ResultSet resultSet = manager.getSession().execute(select);

            PagingState pagingState = resultSet.getExecutionInfo().getPagingState();
            query.setPagingState(pagingState);

            List<ColumnEntity> entities = new ArrayList<>();
            for (Row row : resultSet) {
                entities.add(CassandraConverter.toDocumentEntity(row));
                if (resultSet.getAvailableWithoutFetching() == 0) {
                    query.setExhausted(resultSet.isExhausted());
                    break;
                }
            }
            return entities.stream();
        }

    }, DEFAULT {
        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnFamilyManager manager) {
            return execute(keyspace, query, null, manager);
        }

        @Override
        public Stream<ColumnEntity> execute(String keyspace, ColumnQuery query, ConsistencyLevel level, DefaultCassandraColumnFamilyManager manager) {
            BuiltStatement select = QueryUtils.select(query, keyspace);

            if (Objects.nonNull(level)) {
                select.setConsistencyLevel(level);
            }
            ResultSet resultSet = manager.getSession().execute(select);
            return resultSet.all().stream().map(CassandraConverter::toDocumentEntity);
        }
    }

}
