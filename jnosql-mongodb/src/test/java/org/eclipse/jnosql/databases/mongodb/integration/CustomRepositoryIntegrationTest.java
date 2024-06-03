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
package org.eclipse.jnosql.databases.mongodb.integration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import jakarta.data.Order;
import jakarta.data.Sort;
import jakarta.data.page.Page;
import jakarta.data.page.PageRequest;
import jakarta.inject.Inject;
import org.bson.BsonDocument;
import org.bson.Document;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentConfigurations;
import org.eclipse.jnosql.databases.mongodb.mapping.MongoDBTemplate;
import org.eclipse.jnosql.mapping.Database;
import org.eclipse.jnosql.mapping.DatabaseType;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.core.spi.EntityMetadataExtension;
import org.eclipse.jnosql.mapping.document.DocumentTemplate;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.semistructured.EntityConverter;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.time.Duration;
import java.util.List;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.awaitility.Awaitility.await;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.mongodb.communication.DocumentDatabase.INSTANCE;

@EnableAutoWeld
@AddPackages(value = {Database.class, EntityConverter.class, DocumentTemplate.class, MongoDBTemplate.class})
@AddPackages(Book.class)
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class CustomRepositoryIntegrationTest {

    public static final String DATABASE_NAME = "library";

    static {
        INSTANCE.get(DATABASE_NAME);
        System.setProperty(MongoDBDocumentConfigurations.HOST.get() + ".1", INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), DATABASE_NAME);
    }

    @Inject
    @Database(DatabaseType.DOCUMENT)
    BookCustomRepository bookCustomRepository;


    @BeforeEach
    void cleanUp() {
        try (MongoClient mongoClient = INSTANCE.mongoClient()) {
            MongoCollection<Document> collection = mongoClient.getDatabase(DATABASE_NAME)
                    .getCollection(Book.class.getSimpleName());
            collection.deleteMany(new BsonDocument());
            await().atMost(Duration.ofSeconds(2))
                    .until(() -> collection.find().limit(1).first() == null);
        }
    }

    @Test
    void shouldSave() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(bookCustomRepository.save(book))
                .isNotNull()
                .isEqualTo(book);

        assertThat(bookCustomRepository.getById(book.id()))
                .as("should return the persisted book")
                .hasValue(book);

        Book updated = new Book(book.id(), book.title() + " updated", 2);

        assertThat(bookCustomRepository.save(updated))
                .isNotNull()
                .isNotEqualTo(book);

        assertThat(bookCustomRepository.getById(book.id()))
                .as("should return the updated book")
                .hasValue(updated);
    }

    @Test
    void shouldSaveAllAndFindByIdIn() {

        List<Book> books = List.of(
                new Book(randomUUID().toString(), "Java Persistence Layer", 1)
                , new Book(randomUUID().toString(), "Effective Java", 3)
                , new Book(randomUUID().toString(), "Jakarta EE Cookbook", 1)
                , new Book(randomUUID().toString(), "Mastering The Java Virtual Machine", 1)
        );

        assertThat(bookCustomRepository.saveAll(books))
                .isNotNull()
                .containsAll(books);

        assertThat(bookCustomRepository.findByIdIn(books.stream().map(Book::id).toList()))
                .as("should return the persisted books")
                .containsAll(books);

    }

    @Test
    void shouldSaveAllAndFindBy() {

        Book javaPersistenceLayer = new Book(randomUUID().toString(), "Java Persistence Layer", 1);
        Book effectiveJava = new Book(randomUUID().toString(), "Effective Java", 3);
        Book jakartaEeCookbook = new Book(randomUUID().toString(), "Jakarta EE Cookbook", 1);
        Book masteringTheJavaVirtualMachine = new Book(randomUUID().toString(), "Mastering The Java Virtual Machine", 1);

        List<Book> books = List.of(
                javaPersistenceLayer
                , effectiveJava
                , jakartaEeCookbook
                , masteringTheJavaVirtualMachine
        );

        assertThat(bookCustomRepository.saveAll(books))
                .isNotNull()
                .containsAll(books);

        PageRequest pageRequest = PageRequest.ofSize(2);
        Order<Book> orderByTitleAsc = Order.by(Sort.asc("title"));

        Page<Book> page1 = bookCustomRepository.listAll(pageRequest, orderByTitleAsc);
        Page<Book> page2 = bookCustomRepository.listAll(page1.nextPageRequest(), orderByTitleAsc);
        Page<Book> page3 = bookCustomRepository.listAll(page2.nextPageRequest(), orderByTitleAsc);

        assertSoftly(softly -> {

            softly.assertThat(page1)
                    .as("should return the first page")
                    .hasSize(2)
                    .containsSequence(effectiveJava, jakartaEeCookbook);

            softly.assertThat(page2)
                    .as("should return the first page")
                    .hasSize(2)
                    .containsSequence(javaPersistenceLayer, masteringTheJavaVirtualMachine);

            softly.assertThat(page3)
                    .as("should return the third and last page with no items")
                    .hasSize(0);
        });


    }


    @Test
    void shouldDelete() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(bookCustomRepository.save(book))
                .isNotNull()
                .isEqualTo(book);

        assertThat(bookCustomRepository.getById(book.id()))
                .isNotNull()
                .hasValue(book);

        bookCustomRepository.delete(book);

        assertThat(bookCustomRepository.getById(book.id()))
                .isNotNull()
                .isEmpty();
    }

    @Test
    void shouldDeleteAll() {

        List<Book> books = List.of(
                new Book(randomUUID().toString(), "Java Persistence Layer", 1)
                , new Book(randomUUID().toString(), "Effective Java", 3)
                , new Book(randomUUID().toString(), "Jakarta EE Cookbook", 1)
                , new Book(randomUUID().toString(), "Mastering The Java Virtual Machine", 1)
        );

        assertThat(bookCustomRepository.saveAll(books))
                .isNotNull()
                .containsAll(books);

        await().atMost(Duration.ofSeconds(2))
                .until(() -> bookCustomRepository.listAll().toList().size() >= books.size());

        assertThat(bookCustomRepository.listAll())
                .isNotNull()
                .containsAll(books);

        bookCustomRepository.deleteAll();

        await().atMost(Duration.ofSeconds(2))
                .until(() -> bookCustomRepository.listAll().toList().isEmpty());


        assertThat(bookCustomRepository.listAll())
                .isNotNull()
                .isEmpty();

    }

    @Test
    void shouldRemoveAll() {

        List<Book> books = List.of(
                new Book(randomUUID().toString(), "Java Persistence Layer", 1)
                , new Book(randomUUID().toString(), "Effective Java", 3)
                , new Book(randomUUID().toString(), "Jakarta EE Cookbook", 1)
                , new Book(randomUUID().toString(), "Mastering The Java Virtual Machine", 1)
        );

        assertThat(bookCustomRepository.saveAll(books))
                .isNotNull()
                .containsAll(books);

        await().atMost(Duration.ofSeconds(2))
                .until(() -> bookCustomRepository.listAll().toList().size() >= books.size());

        assertThat(bookCustomRepository.listAll())
                .isNotNull()
                .containsAll(books);

        bookCustomRepository.removeAll(books);

        await().atMost(Duration.ofSeconds(2))
                .until(() -> bookCustomRepository.listAll()
                        .filter(books::contains)
                        .toList().isEmpty());

        assertThat(bookCustomRepository.findByIdIn(books.stream().map(Book::id).toList()))
                .isNotNull()
                .isEmpty();
    }

}
