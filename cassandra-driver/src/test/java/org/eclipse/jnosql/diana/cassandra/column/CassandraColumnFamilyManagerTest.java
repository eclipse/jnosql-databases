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
import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.nosql.NonUniqueResultException;
import jakarta.nosql.Value;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnDeleteQuery;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnQuery;
import jakarta.nosql.column.Columns;
import jakarta.nosql.query.DeleteQuery;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jakarta.nosql.column.ColumnDeleteQuery.delete;
import static jakarta.nosql.column.ColumnQuery.select;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CassandraColumnFamilyManagerTest {

    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private CassandraColumnFamilyManager entityManager;

    @BeforeEach
    public void setUp() {
        CassandraColumnFamilyManagerFactory managerFactory = ManagerFactorySupplier.INSTANCE.get();
        entityManager = managerFactory.get(Constants.KEY_SPACE);
    }

    @AfterEach
    public void afterEach() {
        DefaultCassandraColumnFamilyManager cassandraColumnFamilyManager = DefaultCassandraColumnFamilyManager.class.cast(entityManager);
        CqlSession session = cassandraColumnFamilyManager.getSession();
        if (!session.isClosed()) {
            entityManager.cql("DROP TABLE IF EXISTS " + Constants.KEY_SPACE + '.' + Constants.COLUMN_FAMILY);
        }
    }

    @Test
    public void shouldClose() throws Exception {
        entityManager.close();
        DefaultCassandraColumnFamilyManager cassandraColumnFamilyManager = DefaultCassandraColumnFamilyManager.class.cast(entityManager);
        CqlSession session = cassandraColumnFamilyManager.getSession();
        assertTrue(session.isClosed());
    }

    @Test
    public void shouldInsertJustKey() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(Constants.COLUMN_FAMILY);
        columnEntity.add(key);
        entityManager.insert(columnEntity);
    }


    @Test
    public void shouldInsertColumns() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);
    }

    @Test
    public void shouldInsertWithTtl() throws InterruptedException {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity, Duration.ofSeconds(1L));

        sleep(2_000L);

        List<ColumnEntity> entities = entityManager.select(select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldInsertIterableWithTtl() throws InterruptedException {
        entityManager.insert(getEntities(), Duration.ofSeconds(1L));

        sleep(2_000L);

        List<ColumnEntity> entities = entityManager.select(select().from(Constants.COLUMN_FAMILY).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldReturnErrorWhenInsertWithColumnNull() {

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithConsistencyLevelNull() {

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(getColumnFamily(), null);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert(getEntities(), null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> {
            entityManager.insert((Iterable<ColumnEntity>) null);
        });
    }


    @Test
    public void shouldInsertColumnsWithConsistencyLevel() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.save(columnEntity, CONSISTENCY_LEVEL);
    }


    @Test
    public void shouldReturnErrorWhenUpdateWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> {
            entityManager.update((Iterable<ColumnEntity>) null);
        });

    }

    @Test
    public void shouldReturnErrorWhenUpdateWithColumnNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.update((ColumnEntity) null);
        });
    }

    @Test
    public void shouldUpdateColumn() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.update(columnEntity);
    }

    @Test
    public void shouldUpdateColumns() {
        entityManager.update(getEntities());
    }

    @Test
    public void shouldReturnErrorWhenSaveHasNullElement() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.save((ColumnEntity) null, Duration.ofSeconds(1L), ConsistencyLevel.ALL);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.save(getColumnFamily(), null, ConsistencyLevel.ALL);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.save(getColumnFamily(), Duration.ofSeconds(1L), null);
        });
    }

    @Test
    public void shouldReturnErrorWhenSaveIterableHasNullElement() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.save((List<ColumnEntity>) null, Duration.ofSeconds(1L), ConsistencyLevel.ALL);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.save(getEntities(), null, ConsistencyLevel.ALL);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.save(getEntities(), Duration.ofSeconds(1L), null);
        });
    }

    @Test
    public void shouldFindAll() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);

        ColumnQuery query = select().from(columnEntity.getName()).build();
        List<ColumnEntity> entities = entityManager.select(query).collect(toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldReturnSingleResult() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);
        ColumnQuery query = select().from(columnEntity.getName()).where("id").eq(10L).build();
        Optional<ColumnEntity> entity = entityManager.singleResult(query);

        query = select().from(columnEntity.getName()).where("id").eq(-10L).build();
        entity = entityManager.singleResult(query);
        assertFalse(entity.isPresent());

    }

    @Test
    public void shouldReturnErrorWhenThereIsNotThanOneResultInSingleResult() {
        entityManager.insert(getEntities());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).build();
        assertThrows(NonUniqueResultException.class, () -> {
            entityManager.singleResult(query);
        });
    }

    @Test
    public void shouldReturnErrorWhenQueryIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.select(null);
        });

        assertThrows(NullPointerException.class, () -> {
            entityManager.singleResult(null);
        });
    }


    @Test
    public void shouldFindById() {

        entityManager.insert(getColumnFamily());

        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        List<ColumnEntity> columnEntity = entityManager.select(query).collect(toList());
        assertFalse(columnEntity.isEmpty());
        List<Column> columns = columnEntity.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));

    }

    @Test
    public void shouldFindByIdWithConsistenceLevel() {

        entityManager.insert(getColumnFamily());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        List<ColumnEntity> columnEntity = entityManager.select(query, CONSISTENCY_LEVEL).collect(toList());
        assertFalse(columnEntity.isEmpty());
        List<Column> columns = columnEntity.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));

    }

    @Test
    public void shouldRunNativeQuery() {
        entityManager.insert(getColumnFamily());
        List<ColumnEntity> entities = entityManager.cql("select * from newKeySpace.newColumnFamily where id=10;")
                .collect(toList());
        assertFalse(entities.isEmpty());
        List<Column> columns = entities.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));
    }

    @Test
    public void shouldRunNativeQuery2() {
        entityManager.insert(getColumnFamily());
        String query = "select * from newKeySpace.newColumnFamily where id = :id;";
        List<ColumnEntity> entities = entityManager.cql(query, singletonMap("id", 10L)).collect(toList());
        assertFalse(entities.isEmpty());
        List<Column> columns = entities.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));
    }

    @Test
    public void shouldPrepareStatement() {
        entityManager.insert(getColumnFamily());
        CassandraPreparedStatement preparedStatement = entityManager.nativeQueryPrepare("select * from newKeySpace.newColumnFamily where id=?");
        preparedStatement.bind(10L);
        List<ColumnEntity> entities = preparedStatement.executeQuery().collect(toList());
        List<Column> columns = entities.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));
    }

    @Test
    public void shouldDeleteColumnFamily() {
        entityManager.insert(getColumnFamily());
        ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 10L)));
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        ColumnDeleteQuery deleteQuery = delete().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        entityManager.delete(deleteQuery);
        List<ColumnEntity> entities = entityManager.cql("select * from newKeySpace.newColumnFamily where id=10;")
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldReturnErrorWhenDeleteQueryIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.delete(null);
        });
    }

    @Test
    public void shouldReturnErrorWhenDeleteConsistencyLevelIsNull() {
        assertThrows(NullPointerException.class, () -> {
            entityManager.delete(delete().from(Constants.COLUMN_FAMILY).build(), null);
        });
    }

    @Test
    public void shouldDeleteColumnFamilyWithConsistencyLevel() {
        entityManager.insert(getColumnFamily());
        ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 10L)));
        ColumnDeleteQuery deleteQuery = delete().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        entityManager.delete(deleteQuery, CONSISTENCY_LEVEL);
        List<ColumnEntity> entities = entityManager.cql("select * from newKeySpace.newColumnFamily where id=10;")
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldLimitResult() {
        getEntities().forEach(entityManager::insert);
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").in(Arrays.asList(1L, 2L, 3L))
                .limit(2).build();
        List<ColumnEntity> columnFamilyEntities = entityManager.select(query).collect(toList());
        assertEquals(Integer.valueOf(2), Integer.valueOf(columnFamilyEntities.size()));
    }

    private List<ColumnEntity> getEntities() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "Cassandra");
        fields.put("version", 3.2);
        fields.put("options", asList(1, 2, 3));
        List<Column> columns = Columns.of(fields);
        ColumnEntity entity = ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 1L)));
        ColumnEntity entity1 = ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 2L)));
        ColumnEntity entity2 = ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 3L)));
        columns.forEach(entity::add);
        columns.forEach(entity1::add);
        columns.forEach(entity2::add);
        return asList(entity, entity1, entity2);
    }

    @Test
    public void shouldSupportUDT() {
        ColumnEntity entity = ColumnEntity.of("users");
        entity.add(Column.of("nickname", "ada"));
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder("fullname").withName("name")
                .addUDT(columns).build();
        entity.add(udt);
        entityManager.insert(entity);

        ColumnQuery query = select().from("users").build();
        ColumnEntity columnEntity = entityManager.singleResult(query).get();
        Column column = columnEntity.find("name").get();
        udt = UDT.class.cast(column);
        List<Column> udtColumns = (List<Column>) udt.get();
        assertEquals("name", udt.getName());
        assertEquals("fullname", udt.getUserType());
        assertThat(udtColumns, Matchers.containsInAnyOrder(Column.of("firstname", "Ada"),
                Column.of("lastname", "Lovelace")));
    }

    @Test
    public void shouldSupportAnUDTElement() {
        ColumnEntity entity = ColumnEntity.of("users");
        entity.add(Column.of("nickname", "Ioda"));
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ioda"));
        UDT udt = UDT.builder("fullname").withName("name")
                .addUDT(columns).build();
        entity.add(udt);
        entityManager.insert(entity);

        ColumnQuery query = select().from("users")
                .where("nickname").eq("Ioda")
                .build();

        ColumnEntity columnEntity = entityManager.singleResult(query).get();
        Column column = columnEntity.find("name").get();
        udt = UDT.class.cast(column);
        List<Column> udtColumns = (List<Column>) udt.get();
        assertEquals("name", udt.getName());
        assertEquals("fullname", udt.getUserType());
        assertThat(udtColumns, Matchers.containsInAnyOrder(Column.of("firstname", "Ioda")));
    }


    @Test
    public void shouldSupportDate() {
        ColumnEntity entity = ColumnEntity.of("history");
        entity.add(Column.of("name", "World war II"));
        ZoneId defaultZoneId = ZoneId.systemDefault();
        Instant dateEnd = LocalDate.of(1945, Month.SEPTEMBER, 2).atStartOfDay(defaultZoneId).toInstant();
        LocalDateTime dataStart = LocalDateTime.now();
        entity.add(Column.of("dataStart", LocalDate.of(1939, 9, 1)));
        entity.add(Column.of("dateEnd", dateEnd));
        entityManager.insert(entity);
        ColumnQuery query = select().from("history")
                .where("name").eq("World war II")
                .build();

        ColumnEntity entity1 = entityManager.singleResult(query).get();
        assertNotNull(entity1);

    }

    @Test
    public void shouldSupportListUDTs() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);
    }

    @Test
    public void shouldReturnListUDT() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);

        ColumnQuery query = select().from("contacts").where("user").eq("otaviojava").build();
        ColumnEntity columnEntity = entityManager.singleResult(query).get();
        List<List<Column>> names = (List<List<Column>>) columnEntity.find("names").get().get();
        assertEquals(3, names.size());
        assertTrue(names.stream().allMatch(n -> n.size() == 2));
    }

    @Test
    public void shouldCount() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);
        long contacts = entityManager.count("contacts");
        assertTrue(contacts > 0);
    }

   @Test
    public void shouldPagingState() {
        for (long index = 1; index <= 10; index++) {
            ColumnEntity columnFamily = getColumnFamily();
            columnFamily.add("id", index);
            entityManager.insert(columnFamily);
        }

        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).build();
        CassandraQuery cassandraQuery = CassandraQuery.of(query);

        assertFalse(cassandraQuery.getPagingState().isPresent());

        List<ColumnEntity> entities = entityManager.select(cassandraQuery).collect(toList());
        assertEquals(10, entities.size());
        assertTrue(cassandraQuery.getPagingState().isPresent());
    }

    @Test
    public void shouldPaginate() {
        for (long index = 1; index <= 10; index++) {
            ColumnEntity columnFamily = getColumnFamily();
            columnFamily.add("id", index);
            entityManager.insert(columnFamily);
        }

        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).limit(4).skip(2).build();
        List<ColumnEntity> entities = entityManager.select(query).collect(toList());
        assertEquals(4, entities.size());
    }

    @Test
    public void shouldCreateUDTWithSet() {
        ColumnEntity entity = createEntityWithIterableSet();
        entityManager.insert(entity);
        ColumnQuery query = ColumnQuery.select().from("agenda").build();
        final ColumnEntity result = entityManager.singleResult(query).get();
        Assert.assertEquals(Column.of("user", "otaviojava"), result.find("user").get());
        Assert.assertEquals(2, result.size());
        List<List<Column>> names = (List<List<Column>>) result.find("names").get().get();
        assertEquals(3, names.size());
        assertTrue(names.stream().allMatch(n -> n.size() == 2));
    }

    private ColumnEntity createEntityWithIterable() {
        ColumnEntity entity = ColumnEntity.of("contacts");
        entity.add(Column.of("user", "otaviojava"));

        List<Iterable<Column>> columns = new ArrayList<>();
        columns.add(asList(Column.of("firstname", "Poliana"), Column.of("lastname", "Santana")));
        columns.add(asList(Column.of("firstname", "Ada"), Column.of("lastname", "Lovelace")));
        columns.add(asList(Column.of("firstname", "Maria"), Column.of("lastname", "Goncalves")));
        UDT udt = UDT.builder("fullname").withName("names")
                .addUDTs(columns).build();
        entity.add(udt);
        return entity;
    }

    private ColumnEntity createEntityWithIterableSet() {
        ColumnEntity entity = ColumnEntity.of("agenda");
        entity.add(Column.of("user", "otaviojava"));

        List<Iterable<Column>> columns = new ArrayList<>();
        columns.add(asList(Column.of("firstname", "Poliana"), Column.of("lastname", "Santana")));
        columns.add(asList(Column.of("firstname", "Ada"), Column.of("lastname", "Lovelace")));
        columns.add(asList(Column.of("firstname", "Maria"), Column.of("lastname", "Goncalves")));
        UDT udt = UDT.builder("fullname").withName("names")
                .addUDTs(columns).build();
        entity.add(udt);
        return entity;
    }

    private ColumnEntity getColumnFamily() {
        Map<String, Object> fields = new HashMap<>();
        fields.put("name", "Cassandra");
        fields.put("version", 3.2);
        fields.put("options", asList(1, 2, 3));
        List<Column> columns = Columns.of(fields);
        ColumnEntity columnFamily = ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 10L)));
        columns.forEach(columnFamily::add);
        return columnFamily;
    }

}