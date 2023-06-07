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
package org.eclipse.jnosql.databases.elasticsearch.integration;


import jakarta.inject.Inject;
import org.awaitility.Awaitility;
import org.eclipse.jnosql.databases.elasticsearch.communication.DocumentDatabase;
import org.eclipse.jnosql.databases.elasticsearch.communication.ElasticsearchConfigurations;
import org.eclipse.jnosql.databases.elasticsearch.mapping.ElasticsearchTemplate;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(ElasticsearchTemplate.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class RepositoryIntegrationTest {

    public static final String INDEX = "library";

    static {
        DocumentDatabase instance = DocumentDatabase.INSTANCE;
        instance.get("library");
        System.setProperty(ElasticsearchConfigurations.HOST.get() + ".1", instance.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), INDEX);
        Awaitility.setDefaultPollDelay(100, MILLISECONDS);
        Awaitility.setDefaultTimeout(60L, SECONDS);
    }


    @Inject
    private Library library;

    @BeforeEach
    @AfterEach
    public void clearDatabase() {
        DocumentDatabase.clearDatabase(INDEX);
    }

    @Test
    public void shouldInsert() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        library.save(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = library.findById(book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });
        assertThat(reference.get()).isNotNull().isEqualTo(book);
    }

    @Test
    public void shouldUpdate() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        Book updated = book.updateEdition(2);

        assertThat(library.save(updated))
                .isNotNull()
                .isNotEqualTo(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = library.findById(book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });
        assertThat(reference.get()).isNotNull().isEqualTo(updated);

    }

    @Test
    public void shouldFindById() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);

        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = library.findById(book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });

        assertThat(reference.get()).isNotNull().isEqualTo(book);
    }

    @Test
    public void shouldDelete() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);


        library.deleteById(book.id());

        assertThat(library.findById(book.id()))
                .isNotNull().isEmpty();
    }


    @Test
    public void shouldFindByAuthorName() throws InterruptedException {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);

        Set<Book> expectedBooks = Set.of(book, book.newEdition(), book.newEdition());
        library.saveAll(expectedBooks);

        await().until(() ->
                !library.findByAuthorName(book.author().name()).toList().isEmpty());

        var books = library.findByAuthorName(book.author().name()).toList();
        assertThat(books)
                .hasSize(3);

        assertThat(books)
                .containsAll(expectedBooks);
    }

    @Test
    public void shouldFindByTitleLike() throws InterruptedException {
        Author joshuaBloch = new Author("Joshua Bloch");

        Book effectiveJava1stEdition = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        Book effectiveJava2ndEdition = effectiveJava1stEdition.newEdition();
        Book effectiveJava3rdEdition = effectiveJava2ndEdition.newEdition();

        Author elderMoraes = new Author("Elder Moraes");
        Book jakartaEECookBook = new Book(randomUUID().toString(), "Jakarta EE CookBook", 1, elderMoraes);

        Set<Book> allBooks = Set.of(jakartaEECookBook, effectiveJava1stEdition, effectiveJava2ndEdition, effectiveJava3rdEdition);

        Set<Book> effectiveBooks = Set.of(effectiveJava1stEdition, effectiveJava2ndEdition, effectiveJava3rdEdition);

        library.saveAll(allBooks);

        AtomicReference<List<Book>> booksWithEffective = new AtomicReference<>();
        await().until(() -> {
            var books = library.findByTitleLike("Effective").toList();
            booksWithEffective.set(books);
            return !books.isEmpty();
        });

        AtomicReference<List<Book>> booksWithJa = new AtomicReference<>();
        await().until(() -> {
            var books = library.findByTitleLike("Ja*").toList();
            booksWithJa.set(books);
            return !books.isEmpty();
        });

        assertSoftly(softly -> {
            assertThat(booksWithEffective.get())
                    .as("returned book list with 'Effective' is not equals to the expected items ")
                    .containsAll(effectiveBooks);
        });


        assertSoftly(softly -> {
            assertThat(booksWithJa.get())
                    .as("returned book list with 'Ja*' is not equals to the expected items ")
                    .containsAll(allBooks);
        });
    }


}
