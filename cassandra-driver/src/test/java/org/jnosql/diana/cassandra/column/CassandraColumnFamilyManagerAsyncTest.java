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
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.hamcrest.Matchers;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnQuery;
import org.jnosql.diana.api.column.Columns;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.select;
import static org.jnosql.diana.cassandra.column.Constants.COLUMN_FAMILY;
import static org.jnosql.diana.cassandra.column.Constants.KEY_SPACE;
import static org.junit.jupiter.api.Assertions.assertThrows;


public class CassandraColumnFamilyManagerAsyncTest {

    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private CassandraColumnFamilyManagerAsync columnEntityManager;

    @BeforeAll
    public static void before() throws InterruptedException, IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @AfterAll
    public static void end() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @BeforeEach
    public void setUp() throws InterruptedException, IOException, TTransportException {

        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        CassandraColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.get();
        columnEntityManager = entityManagerFactory.getAsync(KEY_SPACE);
    }

    @Test
    public void shouldInsertJustKeyAsync() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(COLUMN_FAMILY);
        columnEntity.add(key);
        columnEntityManager.insert(columnEntity);
    }

    @Test
    public void shouldReturnErrorWhenInsertColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertIterableColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert((Iterable<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenUpdateColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenUpdateIterableColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert((Iterable<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithTTLNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);

        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert(entity, (Duration) null);
        });

        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert(singletonList(entity), (Duration) null);
        });
    }


    @Test
    public void shouldReturnErrorWhenInsertWithCallbackNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);

        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.insert(entity, (Consumer<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenSaveWithCallbackNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);

        assertThrows(NullPointerException.class, () -> {
            columnEntityManager.update(entity, (Consumer<ColumnEntity>) null);
        });
    }


    @Test
    public void shouldInsertColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        columnEntityManager.insert(columnEntity);
    }

    @Test
    public void shouldInsertColumnsAsyncWithCallBack() {
        ColumnEntity columnEntity = getColumnFamily();
        AtomicBoolean callBack = new AtomicBoolean(false);
        columnEntityManager.insert(columnEntity, c -> callBack.set(true));

        await().untilTrue(callBack);

        ColumnQuery query = select().from(COLUMN_FAMILY).where("id").eq(10L).build();


        final List<ColumnEntity> entities = synchronizedList(new ArrayList<>());

        Consumer<List<ColumnEntity>> result = (l) -> {
            entities.addAll(l);
        };
        columnEntityManager.select(query, result);

        await().until(entities::size, not(equalTo(0)));

        assertThat(entities, Matchers.contains(columnEntity));

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