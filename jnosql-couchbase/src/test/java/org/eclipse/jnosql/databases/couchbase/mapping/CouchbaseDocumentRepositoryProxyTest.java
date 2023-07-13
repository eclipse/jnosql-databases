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
package org.eclipse.jnosql.databases.couchbase.mapping;

import com.couchbase.client.java.json.JsonObject;
import jakarta.inject.Inject;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.query.DocumentRepositoryProducer;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.keyvalue.AbstractKeyValueTemplate;
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@EnableAutoWeld
@AddPackages(value = {Convert.class, AbstractKeyValueTemplate.class,
        DocumentEntityConverter.class, N1QL.class})
@AddPackages(MockProducer.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class, CouchbaseExtension.class})
public class CouchbaseDocumentRepositoryProxyTest {

    private CouchbaseTemplate template;

    @Inject
    private DocumentRepositoryProducer producer;

    private PersonRepository personRepository;


    @BeforeEach
    public void setUp() {
        this.template = Mockito.mock(CouchbaseTemplate.class);

        CouchbaseDocumentRepositoryProxy handler = new CouchbaseDocumentRepositoryProxy(template,
                PersonRepository.class, producer.get(PersonRepository.class, template));

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
        verify(template).n1qlQuery("select * from Person");
    }

    @Test
    public void shouldFindByNameN1ql() {
        ArgumentCaptor<JsonObject> captor = ArgumentCaptor.forClass(JsonObject.class);
        personRepository.findByName("Ada");
        verify(template).n1qlQuery(Mockito.eq("select * from Person where name = $name"), captor.capture());

        JsonObject value = captor.getValue();

        assertEquals("Ada", value.getString("name"));
    }

    interface PersonRepository extends CouchbaseRepository<Person, String> {

        @N1QL("select * from Person")
        List<Person> findAllQuery();

        @N1QL("select * from Person where name = $name")
        List<Person> findByName(@Param("name") String name);
    }
}