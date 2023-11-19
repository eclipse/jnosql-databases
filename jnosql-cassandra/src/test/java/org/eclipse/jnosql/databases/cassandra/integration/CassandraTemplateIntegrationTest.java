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
package org.eclipse.jnosql.databases.cassandra.integration;


import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;
import org.eclipse.jnosql.databases.cassandra.communication.CassandraConfigurations;
import org.eclipse.jnosql.databases.cassandra.communication.ColumnDatabase;
import org.eclipse.jnosql.databases.cassandra.mapping.CassandraTemplate;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.column.ColumnEntityConverter;
import org.eclipse.jnosql.mapping.column.spi.ColumnExtension;
import org.eclipse.jnosql.mapping.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Convert.class, ColumnEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(CassandraTemplate.class)
@AddExtensions({EntityMetadataExtension.class,
        ColumnExtension.class})
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class CassandraTemplateIntegrationTest {

    @Inject
    private CassandraTemplate template;

    static {
        Map<String, Object> configuration = new HashMap<>(ConfigurationReader.from("mapping.properties"));
        for (Map.Entry<String, Object> entry : configuration.entrySet()) {
            System.setProperty(entry.getKey(), entry.getValue().toString());
        }
        System.setProperty(MappingConfigurations.COLUMN_DATABASE.get(), "library");
        System.setProperty(CassandraConfigurations.HOST.get()+".1", ColumnDatabase.INSTANCE.host());
        System.setProperty(CassandraConfigurations.PORT.get(), Integer.toString(ColumnDatabase.INSTANCE.port()));
    }

    @Test
    public void shouldInsert() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.insert(book);
        Optional<Book> optional = template.find(Book.class, book.id());
        assertThat(optional).isNotNull().isNotEmpty()
                .get().isEqualTo(book);
    }

    @Test
    public void shouldUpdate() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        Book updated = new Book(book.id(), book.title() + " updated", 2);

        assertThat(template.update(updated))
                .isNotNull()
                .isNotEqualTo(book);

        assertThat(template.find(Book.class, book.id()))
                .isNotNull().get().isEqualTo(updated);

    }

    @Test
    public void shouldFindById() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        assertThat(template.find(Book.class, book.id()))
                .isNotNull().get().isEqualTo(book);
    }

    @Test
    public void shouldDelete() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        template.delete(Book.class, book.id());
        assertThat(template.find(Book.class, book.id()))
                .isNotNull().isEmpty();
    }


}
