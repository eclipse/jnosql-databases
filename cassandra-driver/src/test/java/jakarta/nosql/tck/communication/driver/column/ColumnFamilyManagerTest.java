package jakarta.nosql.tck.communication.driver.column;

import jakarta.nosql.NonUniqueResultException;
import jakarta.nosql.Value;
import jakarta.nosql.column.Column;
import jakarta.nosql.column.ColumnEntity;
import jakarta.nosql.column.ColumnFamilyManager;
import jakarta.nosql.column.ColumnQuery;
import jakarta.nosql.column.Columns;
import org.eclipse.jnosql.diana.cassandra.column.CassandraColumnFamilyManagerFactory;
import org.eclipse.jnosql.diana.cassandra.column.CassandraQuery;
import org.eclipse.jnosql.diana.cassandra.column.Constants;
import org.eclipse.jnosql.diana.cassandra.column.ManagerFactorySupplier;
import org.eclipse.jnosql.diana.cassandra.column.UDT;
import org.hamcrest.Matchers;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static jakarta.nosql.column.ColumnQuery.select;
import static java.lang.Thread.sleep;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ColumnFamilyManagerTest {

    private ColumnFamilyManager manager;

    @BeforeEach
    public void setUp() {
        CassandraColumnFamilyManagerFactory managerFactory = ManagerFactorySupplier.INSTANCE.get();
        manager = managerFactory.get(Constants.KEY_SPACE);
    }

    @Test
    public void shouldInsertJustKey() {
        Column key = Columns.of("id", 10L);
        ColumnEntity columnEntity = ColumnEntity.of(Constants.COLUMN_FAMILY);
        columnEntity.add(key);
        manager.insert(columnEntity);
    }

    @Test
    public void shouldInsertColumns() {
        ColumnEntity columnEntity = getColumnFamily();
        manager.insert(columnEntity);
    }

    @Test
    public void shouldInsertWithTtl() throws InterruptedException {
        ColumnEntity columnEntity = getColumnFamily();
        manager.insert(columnEntity, Duration.ofSeconds(1L));
        sleep(2_000L);
        List<ColumnEntity> entities = manager.select(select().from(Constants.COLUMN_FAMILY)
                .where("id").eq(10L).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldInsertIterableWithTtl() throws InterruptedException {
        manager.insert(getEntities(), Duration.ofSeconds(1L));

        sleep(2_000L);

        List<ColumnEntity> entities = manager.select(select().from(Constants.COLUMN_FAMILY).build())
                .collect(toList());
        assertTrue(entities.isEmpty());
    }

    @Test
    public void shouldReturnErrorWhenInsertWithColumnNull() {

        assertThrows(NullPointerException.class, () -> {
            manager.insert((ColumnEntity) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithConsistencyLevelNull() {

        assertThrows(NullPointerException.class, () -> {
            manager.insert(getColumnFamily(), null);
        });

        assertThrows(NullPointerException.class, () -> {
            manager.insert(getEntities(), null);
        });
    }

    @Test
    public void shouldReturnErrorWhenInsertWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> {
            manager.insert((Iterable<ColumnEntity>) null);
        });
    }

    @Test
    public void shouldReturnErrorWhenUpdateWithColumnsNull() {

        assertThrows(NullPointerException.class, () -> {
            manager.update((Iterable<ColumnEntity>) null);
        });

    }

    @Test
    public void shouldReturnErrorWhenUpdateWithColumnNull() {
        assertThrows(NullPointerException.class, () -> {
            manager.update((ColumnEntity) null);
        });
    }

    @Test
    public void shouldUpdateColumn() {
        ColumnEntity columnEntity = getColumnFamily();
        manager.update(columnEntity);
    }

    @Test
    public void shouldUpdateColumns() {
        manager.update(getEntities());
    }

    @Test
    public void shouldFindAll() {
        ColumnEntity columnEntity = getColumnFamily();
        manager.insert(columnEntity);

        ColumnQuery query = select().from(columnEntity.getName()).build();
        List<ColumnEntity> entities = manager.select(query).collect(toList());
        assertFalse(entities.isEmpty());
    }

    @Test
    public void shouldReturnSingleResult() {
        ColumnEntity columnEntity = getColumnFamily();
        manager.insert(columnEntity);
        ColumnQuery query = select().from(columnEntity.getName()).where("id").eq(10L).build();
        Optional<ColumnEntity> entity = manager.singleResult(query);

        query = select().from(columnEntity.getName()).where("id").eq(-10L).build();
        entity = manager.singleResult(query);
        assertFalse(entity.isPresent());

    }

    @Test
    public void shouldReturnErrorWhenThereIsNotThanOneResultInSingleResult() {
        manager.insert(getEntities());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).build();
        assertThrows(NonUniqueResultException.class, () -> {
            manager.singleResult(query);
        });
    }

    @Test
    public void shouldReturnErrorWhenQueryIsNull() {
        assertThrows(NullPointerException.class, () -> {
            manager.select(null);
        });

        assertThrows(NullPointerException.class, () -> {
            manager.singleResult(null);
        });
    }

    @Test
    public void shouldFindById() {

        manager.insert(getColumnFamily());
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").eq(10L).build();
        List<ColumnEntity> columnEntity = manager.select(query).collect(toList());
        assertFalse(columnEntity.isEmpty());
        List<Column> columns = columnEntity.get(0).getColumns();
        assertThat(columns.stream().map(Column::getName).collect(toList()), containsInAnyOrder("name", "version", "options", "id"));
        assertThat(columns.stream().map(Column::getValue).map(Value::get).collect(toList()), containsInAnyOrder("Cassandra", 3.2, asList(1, 2, 3), 10L));

    }

    @Test
    public void shouldLimitResult() {
        getEntities().forEach(manager::insert);
        ColumnQuery query = select().from(Constants.COLUMN_FAMILY).where("id").in(Arrays.asList(1L, 2L, 3L))
                .limit(2).build();
        List<ColumnEntity> columnFamilyEntities = manager.select(query).collect(toList());
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
    public void shouldCount() {
        ColumnEntity entity = getColumnFamily();
        manager.insert(entity);
        long contacts = manager.count(Constants.COLUMN_FAMILY);
        assertTrue(contacts > 0);
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
