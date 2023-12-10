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
package org.eclipse.jnosql.databases.solr.mapping;

import jakarta.inject.Inject;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.query.DocumentRepositoryProducer;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
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
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Converters.class,
        DocumentEntityConverter.class, Solr.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class, SolrExtension.class})
public class SolrRepositoryProxyTest {

    private SolrTemplate template;

    @Inject
    private DocumentRepositoryProducer producer;

    @Inject
    private Converters converters;

    @Inject
    private EntitiesMetadata entitiesMetadata;

    private PersonRepository personRepository;

    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(SolrTemplate.class);

        SolrRepositoryProxy handler = new SolrRepositoryProxy(template,
                PersonRepository.class, producer.get(PersonRepository.class, template), converters, entitiesMetadata);

        when(template.insert(any(Person.class))).thenReturn(new Person());
        when(template.insert(any(Person.class), any(Duration.class))).thenReturn(new Person());
        when(template.update(any(Person.class))).thenReturn(new Person());
        personRepository = (PersonRepository) Proxy.newProxyInstance(PersonRepository.class.getClassLoader(),
                new Class[]{PersonRepository.class},
                handler);
    }

    @Test
    public void shouldFindAll() {
        personRepository.findAllQuery();
        verify(template).solr("_entity:person");
    }

    @Test
    public void shouldFindByNameN1ql() {
        ArgumentCaptor<Map> captor = ArgumentCaptor.forClass(Map.class);
        personRepository.findByName("Ada");
        verify(template).solr(Mockito.eq("name:@name AND _entity:person"), captor.capture());

        Map<String, Object> value = captor.getValue();

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

    interface PersonRepository extends SolrRepository<Person, String> {

        @Solr("_entity:person")
        List<Person> findAllQuery();

        @Solr("name:@name AND _entity:person")
        List<Person> findByName(@Param("name") String name);
    }
}