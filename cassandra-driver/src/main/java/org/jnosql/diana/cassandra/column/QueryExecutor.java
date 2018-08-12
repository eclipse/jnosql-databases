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
import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnQuery;

import java.util.ArrayList;
import java.util.List;

interface QueryExecutor {

    List<ColumnEntity> execute(String keyspace, ColumnQuery query, DefaultCassandraColumnFamilyManager manager);


    class SimpleQueryExecutor {

    }

    class PagingStateExecutor implements QueryExecutor {

        @Override
        public List<ColumnEntity> execute(String keyspace, ColumnQuery q, DefaultCassandraColumnFamilyManager manager) {

            CassandraQuery query = CassandraQuery.class.cast(q);
            BuiltStatement select = QueryUtils.select(query, keyspace);
            query.toPatingState().ifPresent(select::setPagingState);
            ResultSet resultSet = manager.getSession().execute(select);

            PagingState pagingState = resultSet.getExecutionInfo().getPagingState();
            query.setPagingState(pagingState);

            List<ColumnEntity> entities = new ArrayList<>();
            for (Row row : resultSet) {
                entities.add(CassandraConverter.toDocumentEntity(row));
                if (resultSet.getAvailableWithoutFetching() == 0) {
                    query.setExhausted(resultSet.isExhausted());
                }
                break;
            }
            return entities;
        }

    }
}
