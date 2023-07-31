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
package org.eclipse.jnosql.databases.orientdb.mapping;

import jakarta.inject.Inject;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.query.DocumentRepositoryProducer;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
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
@AddPackages(value = {Converters.class,
        DocumentEntityConverter.class, SQL.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class, OrientDBExtension.class})
public class OrientDBDocumentRepositoryProxyTest {

    private OrientDBTemplate template;

    @Inject
    private DocumentRepositoryProducer producer;

    private PersonRepository personRepository;


    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(OrientDBTemplate.class);
        PersonRepository personRepository = producer.get(PersonRepository.class, template);
        OrientDBDocumentRepositoryProxy handler = new OrientDBDocumentRepositoryProxy(template,
                PersonRepository.class, personRepository);

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
        verify(template).sql("select * from Person");
    }

    @Test
    public void shouldFindByNameSQL() {
        personRepository.findByName("Ada");
        verify(template).sql(Mockito.eq("select * from Person where name = ?"), Mockito.any(Object.class));
    }

    @Test
    public void shouldFindByNameSQL2() {
        personRepository.findByAge(10);
        ArgumentCaptor<Map> argumentCaptor = ArgumentCaptor.forClass(Map.class);
        verify(template).sql(Mockito.eq("select * from Person where age = :age"), argumentCaptor.capture());
        Map value = argumentCaptor.getValue();
        assertEquals(10, value.get("age"));
    }

    interface PersonRepository extends OrientDBCrudRepository<Person, String> {

        @SQL("select * from Person")
        List<Person> findAllQuery();

        @SQL("select * from Person where name = ?")
        List<Person> findByName(String name);

        @SQL("select * from Person where age = :age")
        List<Person> findByAge(@Param("age") Integer age);
    }
}