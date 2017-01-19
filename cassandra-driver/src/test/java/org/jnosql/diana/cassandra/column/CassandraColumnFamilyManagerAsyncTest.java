/*
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

import com.datastax.driver.core.ConsistencyLevel;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.Columns;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.jnosql.diana.cassandra.column.Constants.COLUMN_FAMILY;
import static org.jnosql.diana.cassandra.column.Constants.KEY_SPACE;


public class CassandraColumnFamilyManagerAsyncTest {

    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private CassandraColumnFamilyManagerAsync columnEntityManager;

    @Before
    public void setUp() {
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        CassandraDocumentEntityManagerFactory entityManagerFactory = cassandraConfiguration.get();
        columnEntityManager = entityManagerFactory.getAsync(KEY_SPACE);
    }

    @Test
    public void shouldInsertJustKeyAsync() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(COLUMN_FAMILY);
        columnEntity.add(key);
        columnEntityManager.save(columnEntity);
    }

    @Test
    public void shouldInsertColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        columnEntityManager.save(columnEntity);
    }

    @Test
    public void shouldInsertColumnsAsyncWithConsistenceLevel() {
        ColumnEntity columnEntity = getColumnFamily();
        columnEntityManager.save(columnEntity, CONSISTENCY_LEVEL);
    }

    private ColumnEntity getColumnFamily() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "Cassandra");
        fields.put("version", 3.2);
        fields.put("options", asList(1, 2, 3));
        List<Column> columns = Columns.of(fields);
        ColumnEntity columnFamily = ColumnEntity.of(COLUMN_FAMILY, singletonList(Columns.of("id", 10L)));
        columns.forEach(columnFamily::add);
        return columnFamily;
    }
}