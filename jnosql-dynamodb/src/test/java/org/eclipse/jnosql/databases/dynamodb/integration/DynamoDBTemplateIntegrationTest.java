/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.integration;

import jakarta.inject.Inject;
import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBConfigurations;
import org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBTestUtils;
import org.eclipse.jnosql.databases.dynamodb.mapping.DynamoDBExtension;
import org.eclipse.jnosql.databases.dynamodb.mapping.DynamoDBTemplate;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(DynamoDBTemplate.class)
@AddExtensions({EntityMetadataExtension.class, DocumentExtension.class, DynamoDBExtension.class})
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DynamoDBTemplateIntegrationTest {

    static {
        DynamoDBTestUtils.CONFIG.setupSystemProperties(Settings.builder()
                .put(DynamoDBConfigurations.CREATE_TABLES, "true"));
    }

    @Inject
    private DynamoDBTemplate template;

    @Test
    void shouldInsert() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.insert(book);
        Optional<Book> optional = template.find(Book.class, book.id());
        assertThat(optional).isNotNull().isNotEmpty()
                .get().isEqualTo(book);
    }

    @Test
    void shouldUpdate() {
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
    void shouldFindById() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        assertThat(template.find(Book.class, book.id()))
                .isNotNull().get().isEqualTo(book);
    }

    @Test
    void shouldDelete() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        template.delete(Book.class, book.id());
        assertThat(template.find(Book.class, book.id()))
                .isNotNull().isEmpty();
    }

    @Test
    void shouldDeleteAll() {
        for (int index = 0; index < 20; index++) {
            Book book = new Book(randomUUID().toString(), "Effective Java", 1);
            assertThat(template.insert(book))
                    .isNotNull()
                    .isEqualTo(book);
        }

        template.delete(Book.class).execute();
        assertThat(template.select(Book.class).result()).isEmpty();
    }

    @Test
    void shouldUpdateNullValues() {
        var book = new Book(randomUUID().toString(), "Effective Java", 1);
        template.insert(book);
        template.update(new Book(book.id(), null, 2));
        Optional<Book> optional = template.select(Book.class).where("id")
                .eq(book.id()).singleResult();
        assertSoftly(softly -> {
            softly.assertThat(optional).isPresent();
            softly.assertThat(optional).get().extracting(Book::title).isNull();
            softly.assertThat(optional).get().extracting(Book::edition).isEqualTo(2);
        });
    }


}
