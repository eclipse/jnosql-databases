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
 *   Maximillian Arruda
 */
package org.eclipse.jnosql.databases.couchbase.integration;


import com.couchbase.client.core.env.TimeoutConfig;
import jakarta.inject.Inject;
import org.eclipse.jnosql.databases.couchbase.communication.CouchbaseUtil;
import org.eclipse.jnosql.databases.couchbase.mapping.CouchbaseExtension;
import org.eclipse.jnosql.databases.couchbase.mapping.CouchbaseTemplate;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.Converters;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(CouchbaseTemplate.class)
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class,
        CouchbaseExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class CouchbaseDocumentRepositoryIntegrationTest {

    @Inject
    private Library library;

    static {
        CouchbaseUtil.systemPropertySetup();
    }

    @BeforeEach
    @AfterEach
    public void cleanUp() {
        library.deleteAll();
    }

    @Test
    public void shouldInsert() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        library.save(book);
        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);
        Optional<Book> optional = library.findById(book.id());
        assertThat(optional).isNotNull().isNotEmpty()
                .get().isEqualTo(book);
    }

    @Test
    public void shouldUpdate() throws InterruptedException {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        Book updated = new Book(book.id(), book.title() + " updated", 2);

        assertThat(library.save(updated))
                .isNotNull()
                .isNotEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        assertThat(library.findById(book.id()))
                .isNotNull().get().isEqualTo(updated);

    }

    @Test
    public void shouldFindById() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);
        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);
        assertThat(library.findById(book.id()))
                .isNotNull().get().isEqualTo(book);
    }

    @Test
    public void shouldFindByTitleLike() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        var data = library.findByTitleLike(book.title()).toList();

        assertSoftly(softly -> {
            softly.assertThat(data).as("query result is a non-null instance").isNotNull();
            softly.assertThat(data.size()).as("query result size is correct").isEqualTo(1);
            softly.assertThat(data.get(0)).as("returned data is correct").isEqualTo(book);
        });
    }

    @Test
    public void shouldFindByEditionLessThan() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        var booksFoundByEditionLessThan = library.findByEditionLessThan(Integer.MAX_VALUE).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionLessThan).as("query result from findByEditionLessThan() is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionLessThan.size()).as("query result size from findByEditionLessThan() is correct").isEqualTo(1);
            softly.assertThat(booksFoundByEditionLessThan.get(0)).as("returned books from findByEditionLessThan() are correct").isEqualTo(book);
        });

        var booksFoundByEditionLessThanZero = library.findByEditionLessThan(0).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionLessThanZero).as("query result from findByEditionLessThan(0) is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionLessThanZero.size()).as("query result size from findByEditionLessThan(0) is correct").isEqualTo(0);
        });

        var booksFoundByEditionGreaterThan = library.findByEditionGreaterThan(0).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionGreaterThan).as("query result from findByEditionGreaterThan() is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionGreaterThan.size()).as("query result size from findByEditionGreaterThan() is correct").isEqualTo(1);
            softly.assertThat(booksFoundByEditionGreaterThan.get(0)).as("returned books from findByEditionGreaterThan() are correct").isEqualTo(book);
        });


        var booksFoundByEditionGreaterThanMaxValue = library.findByEditionGreaterThan(Integer.MAX_VALUE).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionGreaterThanMaxValue).as("query result from findByEditionGreaterThan(Integer.MAX_VALUE) is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionGreaterThanMaxValue.size()).as("query result size from findByEditionGreaterThan(Integer.MAX_VALUE) is correct").isEqualTo(0);
        });
    }

    @Test
    public void shouldFindByEditionGreaterThan() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        var booksFoundByEditionGreaterThanZero = library.findByEditionGreaterThan(0).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionGreaterThanZero).as("query result from findByEditionGreaterThan() is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionGreaterThanZero.size()).as("query result size from findByEditionGreaterThan() is correct").isEqualTo(1);
            softly.assertThat(booksFoundByEditionGreaterThanZero.get(0)).as("returned books from findByEditionGreaterThan() are correct").isEqualTo(book);
        });


        var booksFoundByEditionGreaterThanMaxValue = library.findByEditionGreaterThan(Integer.MAX_VALUE).toList();

        assertSoftly(softly -> {
            softly.assertThat(booksFoundByEditionGreaterThanMaxValue).as("query result from findByEditionGreaterThan(Integer.MAX_VALUE) is a non-null instance").isNotNull();
            softly.assertThat(booksFoundByEditionGreaterThanMaxValue.size()).as("query result size from findByEditionGreaterThan(Integer.MAX_VALUE) is correct").isEqualTo(0);
        });
    }

    @Test
    public void shouldDeleteById() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(library.save(book))
                .isNotNull()
                .isEqualTo(book);

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        library.deleteById(book.id());

        await().atLeast(TimeoutConfig.DEFAULT_KV_DURABLE_TIMEOUT);

        assertThat(library.findById(book.id()))
                .isNotNull().isEmpty();
    }

}
