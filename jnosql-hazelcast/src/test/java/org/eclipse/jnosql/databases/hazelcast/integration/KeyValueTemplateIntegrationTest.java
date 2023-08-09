/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.hazelcast.integration;


import jakarta.inject.Inject;
import jakarta.nosql.keyvalue.KeyValueTemplate;
import org.eclipse.jnosql.databases.hazelcast.mapping.HazelcastTemplate;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.keyvalue.KeyValueEntityConverter;
import org.eclipse.jnosql.mapping.keyvalue.spi.KeyValueExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;

@EnableAutoWeld
@AddPackages(value = {Converters.class, KeyValueEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(Reflections.class)
@AddPackages(HazelcastTemplate.class)
@AddExtensions({EntityMetadataExtension.class,
        KeyValueExtension.class})
class KeyValueTemplateIntegrationTest {

    @Inject
    private KeyValueTemplate template;

    static {
        System.setProperty(MappingConfigurations.KEY_VALUE_DATABASE.get(), "library");
    }

    @Test
    public void shouldPutValue() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.put(book);
        Optional<Book> effective = template.get(book.id(), Book.class);
        assertThat(effective)
                .isNotNull()
                .isPresent()
                .get().isEqualTo(book);
    }

    @Test
    public void shouldGet() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.put(book);
        Optional<Book> effective = template.get(book.id(), Book.class);
        assertThat(effective)
                .isNotNull()
                .isPresent()
                .get().isEqualTo(book);
    }

    @Test
    public void shouldDelete() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.put(book);
        Optional<Book> effective = template.get(book.id(), Book.class);
        assertThat(effective)
                .isNotNull()
                .isPresent()
                .get().isEqualTo(book);
        template.delete(Book.class, book.id());

        assertThat(template.get(book.id(), Book.class))
                .isEmpty();
    }
}
