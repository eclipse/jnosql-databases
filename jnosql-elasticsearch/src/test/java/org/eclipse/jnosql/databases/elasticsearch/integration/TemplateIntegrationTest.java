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
import jakarta.nosql.Convert;
import org.eclipse.jnosql.mapping.Database;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.document.DocumentTemplate;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.semistructured.EntityConverter;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;

@EnableAutoWeld
@AddPackages(value = {Database.class, EntityConverter.class, DocumentTemplate.class})
@AddPackages(Book.class)
@AddPackages(Reflections.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class TemplateIntegrationTest {

    @Inject
    private DocumentTemplate template;

    public static final String INDEX = "library";

    static {
        DocumentDatabase instance = DocumentDatabase.INSTANCE;
        instance.get("library");
        System.setProperty(ElasticsearchConfigurations.HOST.get() + ".1", instance.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), INDEX);
        Awaitility.setDefaultPollDelay(100, MILLISECONDS);
        Awaitility.setDefaultTimeout(60L, SECONDS);
    }


    @BeforeEach
    @AfterEach
    public void clearDatabase(){
        DocumentDatabase.clearDatabase(INDEX);
    }

    @Test
    public void shouldInsert() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        template.insert(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = template.find(Book.class, book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });
        assertThat(reference.get()).isNotNull().isEqualTo(book);
    }

    @Test
    public void shouldUpdate() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        Book updated = book.updateEdition(book.edition() + 1);

        assertThat(template.update(updated))
                .isNotNull()
                .isNotEqualTo(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = template.find(Book.class, book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });
        assertThat(reference.get()).isNotNull().isEqualTo(updated);

    }

    @Test
    public void shouldFindById() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        AtomicReference<Book> reference = new AtomicReference<>();
        await().until(() -> {
            Optional<Book> optional = template.find(Book.class, book.id());
            optional.ifPresent(reference::set);
            return optional.isPresent();
        });

        assertThat(reference.get()).isNotNull().isEqualTo(book);
    }

    @Test
    public void shouldDelete() {
        Author joshuaBloch = new Author("Joshua Bloch");
        Book book = new Book(randomUUID().toString(), "Effective Java", 1, joshuaBloch);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        template.delete(Book.class, book.id());
        assertThat(template.find(Book.class, book.id()))
                .isNotNull().isEmpty();
    }

}
