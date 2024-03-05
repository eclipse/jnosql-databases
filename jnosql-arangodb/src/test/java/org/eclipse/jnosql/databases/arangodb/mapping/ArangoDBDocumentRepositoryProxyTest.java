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
package org.eclipse.jnosql.databases.arangodb.mapping;

import jakarta.inject.Inject;
import org.assertj.core.api.Assertions;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.DocumentTemplate;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.keyvalue.spi.KeyValueExtension;
import org.eclipse.jnosql.mapping.metadata.EntitiesMetadata;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.semistructured.EntityConverter;
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
import java.util.Optional;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Converters.class, EntityConverter.class, DocumentTemplate.class, AQL.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class, KeyValueExtension.class,
        DocumentExtension.class, ArangoDBExtension.class})
public class ArangoDBDocumentRepositoryProxyTest {

    private ArangoDBTemplate template;
    @Inject
    private EntitiesMetadata entitiesMetadata;

    @Inject
    private Converters converters;

    private PersonRepository personRepository;

    @SuppressWarnings("rawtypes")
    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(ArangoDBTemplate.class);

        ArangoDBDocumentRepositoryProxy handler = new ArangoDBDocumentRepositoryProxy<>(template,
                PersonRepository.class, converters, entitiesMetadata);

        when(template.insert(any(Person.class))).thenReturn(new Person());
        when(template.insert(any(Person.class), any(Duration.class))).thenReturn(new Person());
        when(template.update(any(Person.class))).thenReturn(new Person());
        this.personRepository = (PersonRepository) Proxy.newProxyInstance(PersonRepository.class.getClassLoader(),
                new Class[]{PersonRepository.class},
                handler);
    }


    @Test
    public void shouldFindAll() {
        personRepository.findAllQuery();
        verify(template).aql("FOR p IN Person RETURN p", emptyMap());
    }

    @Test
    public void shouldFindByNameAQL() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        personRepository.findByName("Ada");
        verify(template).aql(eq("FOR p IN Person FILTER p.name = @name RETURN p"), captor.capture());

        Map value = captor.getValue();
        assertEquals("Ada", value.get("name"));
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


    interface PersonRepository extends ArangoDBRepository<Person, String> {

        @AQL("FOR p IN Person RETURN p")
        List<Person> findAllQuery();

        @AQL("FOR p IN Person FILTER p.name = @name RETURN p")
        List<Person> findByName(@Param("name") String name);
    }
}
