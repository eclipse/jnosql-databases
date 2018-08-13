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
import com.datastax.driver.core.Session;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.jnosql.diana.api.column.Column;
import org.jnosql.diana.api.column.ColumnDeleteQuery;
import org.jnosql.diana.api.column.ColumnEntity;
import org.jnosql.diana.api.column.ColumnQuery;
import org.jnosql.diana.api.column.Columns;
import org.jnosql.diana.api.column.query.ColumnQueryBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.delete;
import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.select;
import static org.jnosql.diana.cassandra.column.Constants.COLUMN_FAMILY;
import static org.jnosql.diana.cassandra.column.Constants.KEY_SPACE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;


public class CassandraColumnFamilyManagerAsyncTest {

    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;

    private CassandraColumnFamilyManagerAsync entityManager;

    @BeforeAll
    public static void before() throws InterruptedException, IOException, TTransportException {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();
    }

    @AfterAll
    public static void end() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
    }

    @BeforeEach
    public void setUp()  {

        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        CassandraColumnFamilyManagerFactory entityManagerFactory = cassandraConfiguration.get();
        entityManager = entityManagerFactory.getAsync(KEY_SPACE);
    }

    @AfterEach
    public void afterEach() {
        DefaultCassandraColumnFamilyManagerAsync managerAsync = DefaultCassandraColumnFamilyManagerAsync.class.cast(entityManager);
        Session session = managerAsync.getSession();
        if (!session.isClosed()) {
            session.execute("DROP TABLE IF EXISTS " + Constants.KEY_SPACE + '.' + Constants.COLUMN_FAMILY);
        }
    }

    @Test
    public void shouldInsertJustKeyAsync() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(COLUMN_FAMILY);
        columnEntity.add(key);
        entityManager.insert(columnEntity);
    }

    @Test
    public void shouldReturnErrorWhenInsertColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertHasNullElement() {
        final Consumer<ColumnEntity> callBack = c -> {
        };

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(null, Duration.ofSeconds(1L), callBack);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(getColumnFamily(), null, callBack);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(getColumnFamily(), Duration.ofSeconds(1L), null);
        });
    }

    @Test
    public void shouldInsertWithTTl() throws InterruptedException {

        AtomicBoolean callBack = new AtomicBoolean(false);

        entityManager.insert(getColumnFamily(), Duration.ofSeconds(1L), c -> {
            callBack.set(true);
        });
        await().untilTrue(callBack);

        sleep(2_000L);
        callBack.set(false);
        ColumnQuery query = select().from(COLUMN_FAMILY).where("id").eq(10L).build();
        AtomicReference<List<ColumnEntity>> references = new AtomicReference<>();
        entityManager.select(query, l -> {
            references.set(l);
        });

        await().until(references::get, notNullValue(List.class));
        assertTrue(references.get().isEmpty());
    }


    @Test
    public void shouldReturnErrorWhenInsertIterableColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((Iterable<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenUpdateColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenUpdateIterableColumnFamilyIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((Iterable<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithTTLNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(entity, (Duration) null);
        });
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(singletonList(entity), (Duration) null);
        });
    }


    @Test
    public void shouldReturnErrorWhenInsertWithCallbackNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);
        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(entity, (Consumer<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenSaveWithCallbackNull() {
        ColumnEntity entity = ColumnEntity.of(COLUMN_FAMILY);
        assertThrows(NullPointerException.class, () -> {
            entityManager.update(entity, (Consumer<ColumnEntity>) null);
        });
    }


    @Test
    public void shouldInsertColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);
    }

    @Test
    public void shouldInsertIterableColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(singletonList(columnEntity));
    }

    @Test
    public void shouldInsertColumnsAsyncWithCallBack() {
        ColumnEntity columnEntity = getColumnFamily();
        AtomicBoolean callBack = new AtomicBoolean(false);
        entityManager.insert(columnEntity, c -> callBack.set(true));

        await().untilTrue(callBack);

        ColumnQuery query = select().from(COLUMN_FAMILY).where("id").eq(10L).build();

        AtomicReference<List<ColumnEntity>> entities = new AtomicReference<>(emptyList());

        entityManager.select(query, entities::set);
        await().until(() -> entities.get().size(), not(equalTo(0)));
        assertThat(entities.get(), contains(columnEntity));

    }



    @Test
    public void shouldUpdateColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.update(columnEntity);
    }

    @Test
    public void shouldUpdateIterableColumnsAsync() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.update(singletonList(columnEntity));
    }

    @Test
    public void shouldUpdateColumnsAsyncWithCallBack() {
        ColumnEntity columnEntity = getColumnFamily();
        AtomicBoolean callBack = new AtomicBoolean(false);
        entityManager.update(columnEntity, c -> callBack.set(true));

        await().untilTrue(callBack);

        ColumnQuery query = select().from(COLUMN_FAMILY).where("id").eq(10L).build();

        AtomicReference<List<ColumnEntity>> entities = new AtomicReference<>(emptyList());

        entityManager.select(query, entities::set);
        await().until(() -> entities.get().size(), not(equalTo(0)));
        assertThat(entities.get(), contains(columnEntity));

    }



    @Test
    public void shouldReturnErrorWhenDeleteIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.delete(null);
        });
    }

    @Test
    public void shouldReturnErrorWhenCallBackIsNull() {
        ColumnDeleteQuery query = delete().from(COLUMN_FAMILY).build();
        assertThrows(NullPointerException.class, () -> {
            entityManager.delete(query, (Consumer<Void>) null);
        });

    }

    @Test
    public void shouldReturnErrorWhenConsistencyIsNull() {
        ColumnDeleteQuery query = delete().from(COLUMN_FAMILY).build();
        assertThrows(NullPointerException.class, () -> {
            entityManager.delete(query, (ConsistencyLevel) null);
        });

    }

    @Test
    public void shouldDelete() {
        ColumnDeleteQuery query = delete().from(COLUMN_FAMILY).where("id").eq(10L).build();
        entityManager.delete(query);
    }

    @Test
    public void shouldDeleteWithCallBack() {
        AtomicBoolean callback = new AtomicBoolean(false);
        ColumnDeleteQuery deleteQuery = delete().from(COLUMN_FAMILY).where("id").eq(10L).build();
        entityManager.delete(deleteQuery, v -> callback.set(true));

        await().untilTrue(callback);

        ColumnQuery query = select().from(COLUMN_FAMILY).where("id").eq(10L).build();

        AtomicReference<List<ColumnEntity>> entities = new AtomicReference<>(emptyList());
        callback.set(false);
        Consumer<List<ColumnEntity>> result = (l) -> {
            callback.set(true);
            entities.set(l);
        };
        entityManager.select(query, result);
        await().untilTrue(callback);

        assertTrue(entities.get().isEmpty());
    }

    @Test
    public void shouldInsertColumnsAsyncWithConsistenceLevel() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.save(columnEntity, CONSISTENCY_LEVEL);
    }

    @Test
    public void shouldCount() {
        ColumnEntity entity = getColumnFamily();
        entityManager.insert(entity);
        AtomicLong counter = new AtomicLong(0);
        AtomicBoolean condition = new AtomicBoolean(false);

        Consumer<Long> callback = (l) ->{
            condition.set(true);
            counter.set(l);
        };
        entityManager.count(COLUMN_FAMILY, callback);
        await().untilTrue(condition);
        assertTrue(counter.get() > 0);
    }

    @Test
    public void shouldPagingState() throws InterruptedException {

        for (long index = 1; index < 10; index++) {
            ColumnEntity columnFamily = getColumnFamily();
            columnFamily.add("id", index);
            entityManager.insert(columnFamily);
        }

        MILLISECONDS.sleep(500L);

        ColumnQuery query = ColumnQueryBuilder.select().from(COLUMN_FAMILY).limit(6).build();
        CassandraQuery cassandraQuery = CassandraQuery.of(query);

        assertFalse(cassandraQuery.getPagingState().isPresent());

        AtomicReference<List<ColumnEntity>> reference = new AtomicReference<>();

        entityManager.select(cassandraQuery, reference::set);

        await().untilAtomic(reference, notNullValue());

        List<ColumnEntity> entities = reference.get();
        assertEquals(6, entities.size());
        assertTrue(cassandraQuery.getPagingState().isPresent());

        reference.set(null);
        entityManager.select(cassandraQuery, reference::set);

        await().untilAtomic(reference, notNullValue());
        entities = reference.get();
        assertEquals(3, entities.size());
        assertTrue(cassandraQuery.getPagingState().isPresent());

        reference.set(null);
        entityManager.select(cassandraQuery, reference::set);

        await().untilAtomic(reference, notNullValue());
        entities = reference.get();
        assertTrue(entities.isEmpty());

        assertTrue(cassandraQuery.getPagingState().isPresent());

        reference.set(null);
        entityManager.select(cassandraQuery, reference::set);

        await().untilAtomic(reference, notNullValue());
        entities = reference.get();
        assertTrue(entities.isEmpty());
        assertTrue(cassandraQuery.getPagingState().isPresent());


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