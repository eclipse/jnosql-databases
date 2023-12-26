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
package org.eclipse.jnosql.databases.hazelcast.mapping;

import jakarta.inject.Inject;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.keyvalue.AbstractKeyValueTemplate;
import org.eclipse.jnosql.mapping.keyvalue.spi.KeyValueExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;

import java.util.Collection;

import static com.hazelcast.query.Predicates.equal;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnableAutoWeld
@AddPackages(value = {Converters.class, AbstractKeyValueTemplate.class, Query.class})
@AddPackages(MockProducer.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        KeyValueExtension.class, HazelcastExtension.class})
public class DefaultHazelcastTemplateTest {

    @Inject
    private HazelcastTemplate template;

    @Test
    public void shouldRunQuery() {
        Collection<Person> people = template.sql("active");
        assertNotNull(people);
        assertTrue(people.stream().allMatch(Person.class::isInstance));
    }

    @Test
    public void shouldRunQuery2() {
        Collection<Person> people = template.sql("age = :age", singletonMap("age", 10));
        assertNotNull(people);
        assertTrue(people.stream().allMatch(Person.class::isInstance));
    }

    @Test
    public void shouldRunQuery3() {
        Collection<Person> people = template.sql(equal("name",  "Poliana"));
        assertNotNull(people);
        assertTrue(people.stream().allMatch(Person.class::isInstance));
    }

}