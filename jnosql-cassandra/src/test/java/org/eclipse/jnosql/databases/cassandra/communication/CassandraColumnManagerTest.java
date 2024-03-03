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

package org.eclipse.jnosql.databases.cassandra.communication;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlSession;
import jakarta.data.exceptions.NonUniqueResultException;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.communication.Value;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.communication.column.ColumnDeleteQuery;
import org.eclipse.jnosql.communication.column.ColumnEntity;
import org.eclipse.jnosql.communication.column.ColumnQuery;
import org.eclipse.jnosql.communication.column.Columns;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.column.ColumnDeleteQuery.delete;
import static org.eclipse.jnosql.communication.column.ColumnQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
public class CassandraColumnManagerTest {

    public static final ConsistencyLevel CONSISTENCY_LEVEL = ConsistencyLevel.ONE;
    private CassandraColumnManager entityManager;


    {
        CassandraColumnManagerFactory managerFactory = ColumnDatabase.INSTANCE.get();
        entityManager = managerFactory.apply(Constants.KEY_SPACE);
    }

    @AfterEach
    void afterEach() {
        DefaultCassandraColumnManager manager = DefaultCassandraColumnManager.class.cast(entityManager);
        CqlSession session = manager.getSession();
        if (!session.isClosed()) {
            entityManager.cql("TRUNCATE " + Constants.KEY_SPACE + '.' + Constants.COLUMN_FAMILY);
            entityManager.cql("TRUNCATE " + Constants.KEY_SPACE + '.' + "users");
        }
    }

    @Test
    void shouldClose() throws Exception {
        entityManager.close();
        DefaultCassandraColumnManager manager = DefaultCassandraColumnManager.class.cast(entityManager);
        CqlSession session = manager.getSession();
        assertTrue(session.isClosed());
    }

    @Test
    void shouldInsertJustKey() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(Constants.COLUMN_FAMILY);
        columnEntity.add(key);
        entityManager.insert(columnEntity);
    }


    @Test
    void shouldInsertColumns() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);
    }

    @Test
    void shouldInsertWithTtl() throws InterruptedException {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity, Duration.ofSeconds(1L));

        sleep(2_000L);

        List<ColumnEntity> entities = entityManager.select(select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldInsertIterableWithTtl() throws InterruptedException {
        entityManager.insert(getEntities(), Duration.ofSeconds(1L));

        sleep(2_000L);

        List<ColumnEntity> entities = entityManager.select(select().from(Constants.COLUMN_FAMILY).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldReturnErrorWhenInsertWithColumnNull() {

        assertThrows(NullPointerException.class, () -> entityManager.insert((ColumnEntity) null));
    }

    @Test
    void shouldReturnErrorWhenInsertWithConsistencyLevelNull() {

        assertThrows(NullPointerException.class, () -> entityManager.insert(getColumnFamily(), null));

        assertThrows(NullPointerException.class, () -> entityManager.insert(getEntities(), null));
    }

    @Test
    void shouldReturnErrorWhenInsertWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> entityManager.insert((Iterable<ColumnEntity>) null));
    }


    @Test
    void shouldInsertColumnsWithConsistencyLevel() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.save(columnEntity, CONSISTENCY_LEVEL);
    }


    @Test
    void shouldReturnErrorWhenUpdateWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> entityManager.update((Iterable<ColumnEntity>) null));

    }

    @Test
    void shouldReturnErrorWhenUpdateWithColumnNull() {
        assertThrows(NullPointerException.class, () -> entityManager.update((ColumnEntity) null));
    }

    @Test
    void shouldUpdateColumn() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.update(columnEntity);
    }

    @Test
    void shouldUpdateColumns() {
        entityManager.update(getEntities());
    }

    @Test
    void shouldReturnErrorWhenSaveHasNullElement() {
        assertThrows(NullPointerException.class, () -> entityManager.save((ColumnEntity) null, Duration.ofSeconds(1L), ConsistencyLevel.ALL));

        assertThrows(NullPointerException.class, () -> entityManager.save(getColumnFamily(), null, ConsistencyLevel.ALL));

        assertThrows(NullPointerException.class, () -> entityManager.save(getColumnFamily(), Duration.ofSeconds(1L), null));
    }

    @Test
    void shouldReturnErrorWhenSaveIterableHasNullElement() {
        assertThrows(NullPointerException.class, () -> entityManager.save((List<ColumnEntity>) null, Duration.ofSeconds(1L), ConsistencyLevel.ALL));

        assertThrows(NullPointerException.class, () -> entityManager.save(getEntities(), null, ConsistencyLevel.ALL));

        assertThrows(NullPointerException.class, () -> entityManager.save(getEntities(), Duration.ofSeconds(1L), null));
    }

    @Test
    void shouldFindAll() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);

        ColumnQuery query = select().from(columnEntity.name()).build();
        List<ColumnEntity> entities = entityManager.select(query).collect(toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    void shouldReturnSingleResult() {
        ColumnEntity columnEntity = getColumnFamily();
        entityManager.insert(columnEntity);
        ColumnQuery query = select().from(columnEntity.name()).where("id").eq(10L).build();
        Optional<ColumnEntity> entity = entityManager.singleResult(query);

        query = select().from(columnEntity.name()).where("id").eq(-10L).build();
        entity = entityManager.singleResult(query);
        assertFalse(entity.isPresent());

    }

    @Test
    void shouldReturnErrorWhenThereIsNotThanOneResultInSingleResult() {
        entityManager.insert(getEntities());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).build();
        assertThrows(NonUniqueResultException.class, () -> entityManager.singleResult(query));
    }

    @Test
    void shouldReturnErrorWhenQueryIsNull() {
        assertThrows(NullPointerException.class, () -> entityManager.select(null));

        assertThrows(NullPointerException.class, () -> entityManager.singleResult(null));
    }


    @Test
    void shouldFindById() {

        entityManager.insert(getColumnFamily());

        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        List<ColumnEntity> columnEntity = entityManager.select(query).collect(toList());
        assertFalse(columnEntity.isEmpty());
        List<Column> columns = columnEntity.get(0).columns();
        assertThat(columns.stream().map(Column::name).collect(toList()))
                .contains("name", "version", "options", "id");
        assertThat(columns.stream().map(Column::value).map(Value::get).collect(toList())).contains
                ("Cassandra", 3.2, asList(1, 2, 3), 10L);

    }

    @Test
    void shouldFindByIdWithConsistenceLevel() {

        entityManager.insert(getColumnFamily());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        List<ColumnEntity> columnEntity = entityManager.select(query, CONSISTENCY_LEVEL).collect(toList());
        assertFalse(columnEntity.isEmpty());
        List<Column> columns = columnEntity.get(0).columns();
        assertThat(columns.stream().map(Column::name).collect(toList())).contains("name", "version", "options", "id");
        assertThat(columns.stream().map(Column::value).map(Value::get).collect(toList()))
                .contains("Cassandra", 3.2, asList(1, 2, 3), 10L);

    }

    @Test
    void shouldRunNativeQuery() {
        entityManager.insert(getColumnFamily());
        List<ColumnEntity> entities = entityManager.cql("select * from newKeySpace.newColumnFamily where id=10;")
                .collect(toList());
        assertFalse(entities.isEmpty());
        List<Column> columns = entities.get(0).columns();
        assertThat(columns.stream().map(Column::name).collect(toList())).contains("name", "version", "options", "id");
        assertThat(columns.stream().map(Column::value).map(Value::get).collect(toList()))
                .contains("Cassandra", 3.2, asList(1, 2, 3), 10L);
    }

    @Test
    void shouldRunNativeQuery2() {
        entityManager.insert(getColumnFamily());
        String query = "select * from newKeySpace.newColumnFamily where id = :id;";
        List<ColumnEntity> entities = entityManager.cql(query, singletonMap("id", 10L)).collect(toList());
        assertFalse(entities.isEmpty());
        List<Column> columns = entities.get(0).columns();
        assertThat(columns.stream().map(Column::name).collect(toList()))
                .contains("name", "version", "options", "id");
        assertThat(columns.stream().map(Column::value).map(Value::get).collect(toList()))
                .contains("Cassandra", 3.2, asList(1, 2, 3), 10L);
    }

    @Test
    void shouldPrepareStatement() {
        entityManager.insert(getColumnFamily());
        CassandraPreparedStatement preparedStatement = entityManager.nativeQueryPrepare("select * from newKeySpace.newColumnFamily where id=?");
        preparedStatement.bind(10L);
        List<ColumnEntity> entities = preparedStatement.executeQuery().collect(toList());
        List<Column> columns = entities.get(0).columns();
        assertThat(columns.stream().map(Column::name).collect(toList()))
                .contains("name", "version", "options", "id");
        assertThat(columns.stream().map(Column::value).map(Value::get)
                .collect(toList())).contains("Cassandra", 3.2, asList(1, 2, 3), 10L);
    }

    @Test
    void shouldDeleteColumnFamily() {
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
    void shouldReturnErrorWhenDeleteQueryIsNull() {
        assertThrows(NullPointerException.class, () -> entityManager.delete(null));
    }

    @Test
    void shouldReturnErrorWhenDeleteConsistencyLevelIsNull() {
        assertThrows(NullPointerException.class, () -> entityManager.delete(delete().from(Constants.COLUMN_FAMILY).build(), null));
    }

    @Test
    void shouldDeleteColumnFamilyWithConsistencyLevel() {
        entityManager.insert(getColumnFamily());
        ColumnEntity.of(Constants.COLUMN_FAMILY, singletonList(Columns.of("id", 10L)));
        ColumnDeleteQuery deleteQuery = delete().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        entityManager.delete(deleteQuery, CONSISTENCY_LEVEL);
        List<ColumnEntity> entities = entityManager.cql("select * from newKeySpace.newColumnFamily where id=10;")
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    void shouldLimitResult() {
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
    void shouldSupportUDT() {
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
        assertEquals("name", udt.name());
        assertEquals("fullname", udt.userType());
        assertThat(udtColumns).contains(Column.of("firstname", "Ada"),
                Column.of("lastname", "Lovelace"));
    }

    @Test
    void shouldSupportAnUDTElement() {
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
        assertEquals("name", udt.name());
        assertEquals("fullname", udt.userType());
        assertThat(udtColumns).contains(Column.of("firstname", "Ioda"));
    }


    @Test
    void shouldSupportDate() {
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
    void shouldSupportListUDTs() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);
    }

    @Test
    void shouldReturnListUDT() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);

        ColumnQuery query = select().from("contacts").where("user").eq("otaviojava").build();
        ColumnEntity columnEntity = entityManager.singleResult(query).get();
        List<List<Column>> names = (List<List<Column>>) columnEntity.find("names").get().get();
        assertEquals(3, names.size());
        assertTrue(names.stream().allMatch(n -> n.size() == 2));
    }

    @Test
    void shouldCount() {
        ColumnEntity entity = createEntityWithIterable();
        entityManager.insert(entity);
        long contacts = entityManager.count("contacts");
        assertTrue(contacts > 0);
    }

   @Test
    void shouldPagingState() {
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
    void shouldPaginate() {
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
    void shouldCreateUDTWithSet() {
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

    @Test
    void shouldInsertNullValues(){
        var family = getColumnFamily();
        family.add(Column.of("name", null));
        ColumnEntity columnEntity = entityManager.insert(family);
        var column = columnEntity.find("name");
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(column).get().extracting(Column::name).isEqualTo("name");
            soft.assertThat(column).get().extracting(Column::get).isNull();
        });
    }

    @Test
    void shouldUpdateNullValues(){
        var family = getColumnFamily();
        entityManager.insert(family);
        family.addNull("name");
        ColumnEntity columnEntity = entityManager.update(family);
        var column = columnEntity.find("name");
        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(column).get().extracting(Column::name).isEqualTo("name");
            soft.assertThat(column).get().extracting(Column::get).isNull();
        });
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