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
package org.eclipse.jnosql.databases.arangodb.integration;


import jakarta.inject.Inject;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.databases.arangodb.communication.ArangoDBConfigurations;
import org.eclipse.jnosql.databases.arangodb.mapping.ArangoDBTemplate;
import org.eclipse.jnosql.mapping.Database;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.document.DocumentTemplate;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.semistructured.EntityConverter;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.arangodb.communication.DocumentDatabase.INSTANCE;

@EnableAutoWeld
@AddPackages(value = {Database.class, EntityConverter.class, DocumentTemplate.class})
@AddPackages(Book.class)
@AddPackages(ArangoDBTemplate.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class ArangoDBTemplateIntegrationTest {

    @Inject
    private ArangoDBTemplate template;

    static {
        INSTANCE.get("library");
        System.setProperty(ArangoDBConfigurations.HOST.get() + ".1", INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), "library");
    }

    @BeforeEach
    void setUp() {
        this.template.delete(Book.class).execute();
    }

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
        SoftAssertions.assertSoftly(softly -> {
            softly.assertThat(optional).isPresent();
            softly.assertThat(optional).get().extracting(Book::title).isNull();
            softly.assertThat(optional).get().extracting(Book::edition).isEqualTo(2);
        });
    }

    @Test
    void shouldExecuteLimit() {

        for (int index = 1; index < 10; index++) {
            var book = new Book(randomUUID().toString(), "Effective Java", index);
            template.insert(book);
        }

        List<Book> books = template.select(Book.class).orderBy("edition")
                .asc().limit(4).result();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(books).hasSize(4);
            var editions = books.stream().map(Book::edition).toList();
            soft.assertThat(editions).hasSize(4).contains(1, 2, 3, 4);
        });

    }

    @Test
    void shouldExecuteSkip() {
        for (int index = 1; index < 10; index++) {
            var book = new Book(randomUUID().toString(), "Effective Java", index);
            template.insert(book);
        }

        List<Book> books = template.select(Book.class).orderBy("edition")
                .asc().skip(4).result();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(books).hasSize(5);
            var editions = books.stream().map(Book::edition).toList();
            soft.assertThat(editions).hasSize(5).contains(5, 6, 7, 8, 9);
        });
    }

    @Test
    void shouldExecuteLimitStart() {
        for (int index = 1; index < 10; index++) {
            var book = new Book(randomUUID().toString(), "Effective Java", index);
            template.insert(book);
        }

        List<Book> books = template.select(Book.class).orderBy("edition")
                .asc().skip(4).limit(3).result();

        SoftAssertions.assertSoftly(soft -> {
            soft.assertThat(books).hasSize(3);
            var editions = books.stream().map(Book::edition).toList();
            soft.assertThat(editions).hasSize(3).contains(5, 6, 7);
        });
    }
}
