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
package org.eclipse.jnosql.databases.cassandra.mapping;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.assertj.core.api.Assertions;
import org.eclipse.jnosql.communication.column.Column;
import org.eclipse.jnosql.communication.column.ColumnDeleteQuery;
import org.eclipse.jnosql.communication.column.ColumnEntity;
import org.eclipse.jnosql.communication.column.ColumnQuery;
import org.eclipse.jnosql.databases.cassandra.communication.CassandraColumnManager;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.ColumnEntityConverter;
import org.eclipse.jnosql.mapping.column.ColumnEventPersistManager;
import org.eclipse.jnosql.mapping.column.spi.ColumnExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.column.ColumnQuery.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableAutoWeld
@AddPackages(value = {Converters.class, ColumnEntityConverter.class,
        CQL.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        ColumnExtension.class, CassandraExtension.class})
public class DefaultCassandraTemplateTest {

    @Inject
    private CassandraColumnEntityConverter converter;

    @Inject
    private ColumnEventPersistManager persistManager;

    @Inject
    private EntitiesMetadata entities;

    @Inject
    private Converters converters;

    private CassandraTemplate template;

    private CassandraColumnManager manager;

    @BeforeEach
    void setUp() {
        this.manager = mock(CassandraColumnManager.class);
        Instance instance = mock(Instance.class);
        when(instance.get()).thenReturn(manager);
        template = new DefaultCassandraTemplate(instance, converter, persistManager, entities, converters);
    }

    @Test
    void shouldSaveConsistency() {
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));
        entity.addNull("home");
        ArgumentCaptor<ColumnEntity> captor = ArgumentCaptor.forClass(ColumnEntity.class);

        ConsistencyLevel level = ConsistencyLevel.THREE;

        when(manager.
                save(Mockito.any(ColumnEntity.class), Mockito.eq(level)))
                .thenReturn(entity);

        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        assertEquals(person, template.save(person, level));

        Mockito.verify(manager).save(captor.capture(), Mockito.eq(level));
        assertEquals(entity, captor.getValue());

    }

    @Test
    void shouldSaveConsistencyIterable() {
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));
        entity.addNull("home");
        ArgumentCaptor<ColumnEntity> captor = ArgumentCaptor.forClass(ColumnEntity.class);

        ConsistencyLevel level = ConsistencyLevel.THREE;

        when(manager.
                save(Mockito.any(ColumnEntity.class), Mockito.eq(level)))
                .thenReturn(entity);

        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        assertThat(template.save(Collections.singletonList(person), level)).contains(person);
        Mockito.verify(manager).save(captor.capture(), Mockito.eq(level));
        assertEquals(entity, captor.getValue());

    }

    @Test
    void shouldSaveConsntencyDuration() {
        Duration duration = Duration.ofHours(2);
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));
        entity.addNull("home");
        ArgumentCaptor<ColumnEntity> captor = ArgumentCaptor.forClass(ColumnEntity.class);

        ConsistencyLevel level = ConsistencyLevel.THREE;
        when(manager.
                save(Mockito.any(ColumnEntity.class), Mockito.eq(duration),
                        Mockito.eq(level)))
                .thenReturn(entity);

        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        assertEquals(person, template.save(person, duration, level));

        Mockito.verify(manager).save(captor.capture(), Mockito.eq(duration), Mockito.eq(level));
        assertEquals(entity, captor.getValue());
    }

    @Test
    void shouldSaveConsistencyDurationIterable() {
        Duration duration = Duration.ofHours(2);
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));
        entity.addNull("home");
        ArgumentCaptor<ColumnEntity> captor = ArgumentCaptor.forClass(ColumnEntity.class);

        ConsistencyLevel level = ConsistencyLevel.THREE;
        when(manager.
                save(Mockito.any(ColumnEntity.class), Mockito.eq(duration),
                        Mockito.eq(level)))
                .thenReturn(entity);

        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        assertThat(template.save(Collections.singletonList(person), duration, level)).contains(person);
        Mockito.verify(manager).save(captor.capture(), Mockito.eq(duration), Mockito.eq(level));
        assertEquals(entity, captor.getValue());
    }

    @Test
    void shouldDelete() {


        ColumnDeleteQuery query = ColumnDeleteQuery.delete().from("columnFamily").build();
        ConsistencyLevel level = ConsistencyLevel.THREE;
        template.delete(query, level);
        verify(manager).delete(query, level);
    }


    @Test
    void shouldFind() {
        Person person = new Person();
        person.setName("Name");
        person.setAge(20);

        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));
        ColumnQuery query = select().from("columnFamily").build();
        ConsistencyLevel level = ConsistencyLevel.THREE;
        when(manager.select(query, level)).thenReturn(Stream.of(entity));

        Stream<Person> people = template.find(query, level);
        assertThat(people.collect(Collectors.toList())).contains(person);
    }

    @Test
    void shouldFindCQL() {
        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        String cql = "select * from Person";
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));

        when(manager.cql(cql)).thenReturn(Stream.of(entity));

        List<Person> people = template.<Person>cql(cql).collect(Collectors.toList());
        Assertions.assertThat(people).contains(person);
    }

    @Test
    void shouldFindSimpleStatement() {
        SimpleStatement statement = QueryBuilder.selectFrom("Person").all().build();
        Person person = new Person();
        person.setName("Name");
        person.setAge(20);
        ColumnEntity entity = ColumnEntity.of("Person", asList(Column.of("name", "Name"), Column.of("age", 20)));

        when(manager.execute(statement)).thenReturn(Stream.of(entity));

        List<Person> people = template.<Person>execute(statement).collect(Collectors.toList());
        Assertions.assertThat(people).contains(person);
    }

}
