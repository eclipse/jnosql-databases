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
package org.eclipse.jnosql.databases.oracle.mapping;

import jakarta.data.repository.Param;
import jakarta.inject.Inject;
import org.assertj.core.api.Assertions;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.keyvalue.spi.KeyValueExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.Reflections;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Converters.class, DocumentEntityConverter.class, SQL.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class, KeyValueExtension.class,
        DocumentExtension.class, OracleExtension.class})
public class OracleDocumentRepositoryProxyTest {

    private OracleNoSQLTemplate template;
    @Inject
    private EntitiesMetadata entitiesMetadata;

    @Inject
    private Converters converters;

    private PersonNoSQLRepository personRepository;

    @SuppressWarnings("rawtypes")
    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(OracleNoSQLTemplate.class);

        OracleDocumentRepositoryProxy handler = new OracleDocumentRepositoryProxy<>(template,
                PersonNoSQLRepository.class, converters, entitiesMetadata);

        when(template.insert(any(Person.class))).thenReturn(new Person());
        when(template.insert(any(Person.class), any(Duration.class))).thenReturn(new Person());
        when(template.update(any(Person.class))).thenReturn(new Person());
        this.personRepository = (PersonNoSQLRepository) Proxy.newProxyInstance(PersonNoSQLRepository.class.getClassLoader(),
                new Class[]{PersonNoSQLRepository.class},
                handler);
    }


    @Test
    public void shouldFindAll() {
        personRepository.findAllQuery();
        verify(template).sql("select * from Person");
    }

    @Test
    public void shouldFindByNameSQL() {
        ArgumentCaptor<Object[]> captor = ArgumentCaptor.forClass(Object[].class);
        personRepository.findByName("Ada");
        verify(template).sql(eq("select * from Person where content.name= ?"), captor.capture());

        Object[] value = captor.getValue();
        Assertions.assertThat(value).hasSize(1).contains("Ada");
    }

    @Test
    public void shouldSaveUsingInsert() {
        Person person = Person.of("Ada", 10);
        personRepository.save(person);
        verify(template).insert(eq(person));
    }


    @Test
    public void shouldSaveUsingUpdate() {
        Person person = Person.of("Ada-2", 10);
        when(template.find(Person.class, "Ada-2")).thenReturn(Optional.of(person));
        personRepository.save(person);
        verify(template).update(eq(person));
    }

    @Test
    public void shouldDelete(){
        personRepository.deleteById("id");
        verify(template).delete(Person.class, "id");
    }


    @Test
    public void shouldDeleteEntity(){
        Person person = Person.of("Ada", 10);
        personRepository.delete(person);
        verify(template).delete(Person.class, person.getName());
    }

    @Test
    public void shouldDeleteAll() {
        ArgumentCaptor<Class<?>> queryCaptor = ArgumentCaptor.forClass(Class.class);

        personRepository.deleteAll();
        verify(template).deleteAll(queryCaptor.capture());

        Class<?> query = queryCaptor.getValue();
        Assertions.assertThat(query).isEqualTo(Person.class);
    }


    interface PersonNoSQLRepository extends OracleNoSQLRepository<Person, String> {

        @SQL("select * from Person")
        List<Person> findAllQuery();

        @SQL("select * from Person where content.name= ?")
        List<Person> findByName(@Param("") String name);
    }
}
