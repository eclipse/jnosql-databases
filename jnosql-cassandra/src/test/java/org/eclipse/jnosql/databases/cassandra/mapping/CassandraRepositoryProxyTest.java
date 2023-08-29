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

import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.column.ColumnDeleteQuery;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.JNoSQLColumnTemplate;
import org.eclipse.jnosql.mapping.column.query.ColumnRepositoryProducer;
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

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableAutoWeld
@AddPackages(value = {Converters.class, JNoSQLColumnTemplate.class,
        CQL.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        ColumnExtension.class, CassandraExtension.class})
public class CassandraRepositoryProxyTest {

    private CassandraTemplate template;

    @Inject
    private ColumnRepositoryProducer producer;

    @Inject
    private Converters converters;

    @Inject
    private EntitiesMetadata entitiesMetadata;

    private PersonRepository personRepository;

    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(CassandraTemplate.class);
        PersonRepository personRepository = producer.get(PersonRepository.class, template);
        CassandraRepositoryProxy handler = new CassandraRepositoryProxy(template,
                PersonRepository.class, personRepository, converters, entitiesMetadata);

        when(template.insert(any(Person.class))).thenReturn(new Person());
        when(template.insert(any(Person.class), any(Duration.class))).thenReturn(new Person());
        when(template.update(any(Person.class))).thenReturn(new Person());
        this.personRepository = (PersonRepository) Proxy.newProxyInstance(PersonRepository.class.getClassLoader(),
                new Class[]{PersonRepository.class},
                handler);
    }


    @Test
    public void shouldFindByName() {
        personRepository.findByName("Ada");
        verify(template).cql("select * from Person where name = ?", "Ada");
    }

    @Test
    public void shouldDeleteByName() {
        personRepository.deleteByName("Ada");
        verify(template).delete(Mockito.any(ColumnDeleteQuery.class));
    }

    @Test
    public void shouldFindAll() {
        personRepository.findAllQuery();
        verify(template).cql("select * from Person");
    }

    @Test
    public void shouldFindByNameCQL() {
        personRepository.findByName("Ada");
        verify(template).cql(Mockito.eq("select * from Person where name = ?"), Mockito.any(Object.class));
    }

    @Test
    public void shouldFindByName2CQL() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);

        personRepository.findByName2("Ada");
        verify(template).cql(Mockito.eq("select * from Person where name = :name"), captor.capture());
        Map map = captor.getValue();
        assertEquals("Ada", map.get("name"));
    }

    interface PersonRepository extends CassandraRepository<Person, String> {

        void deleteByName(String namel);

        @CQL("select * from Person")
        List<Person> findAllQuery();

        @CQL("select * from Person where name = ?")
        List<Person> findByName(String name);

        @CQL("select * from Person where name = :name")
        List<Person> findByName2(@Param("name") String name);
    }

}