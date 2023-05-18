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
package org.eclipse.jnosql.databases.mongodb.integration;


import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Alternative;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.nosql.Template;
import jakarta.nosql.document.DocumentTemplate;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.databases.mongodb.communication.DocumentDatabase;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentConfigurations;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentManager;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddBeanClasses;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.jboss.weld.junit5.auto.ExcludeBean;
import org.jboss.weld.junit5.auto.ExcludeBeanClasses;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.mongodb.communication.DocumentDatabase.INSTANCE;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Book.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class TemplateIntegrationTest {

    @Inject
    private Template template;

    static {
        INSTANCE.get("library");
        System.setProperty(MongoDBDocumentConfigurations.HOST.get() + ".1", INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), "library");
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

    }

    @Test
    public void shouldFindById() {

    }


}
