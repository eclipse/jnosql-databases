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


import jakarta.inject.Inject;
import jakarta.nosql.document.DocumentTemplate;
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.databases.mongodb.communication.MongoDBDocumentConfigurations;
import org.eclipse.jnosql.mapping.Convert;
import org.eclipse.jnosql.mapping.core.Converters;
import org.eclipse.jnosql.mapping.config.MappingConfigurations;
import org.eclipse.jnosql.mapping.document.DocumentEntityConverter;
import org.eclipse.jnosql.mapping.document.spi.DocumentExtension;
import org.eclipse.jnosql.mapping.reflection.Reflections;
import org.eclipse.jnosql.mapping.spi.EntityMetadataExtension;
import org.jboss.weld.junit5.auto.AddExtensions;
import org.jboss.weld.junit5.auto.AddPackages;
import org.jboss.weld.junit5.auto.EnableAutoWeld;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.List;
import java.util.Optional;

import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.mongodb.communication.DocumentDatabase.INSTANCE;
import static org.eclipse.jnosql.databases.mongodb.integration.StepTransitionReason.REPEAT;

@EnableAutoWeld
@AddPackages(value = {Convert.class, DocumentEntityConverter.class})
@AddPackages(Book.class)
@AddPackages(Reflections.class)
@AddPackages(Converters.class)
@AddExtensions({EntityMetadataExtension.class,
        DocumentExtension.class})
@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class TemplateIntegrationTest {

    static {
        INSTANCE.get("library");
        System.setProperty(MongoDBDocumentConfigurations.HOST.get() + ".1", INSTANCE.host());
        System.setProperty(MappingConfigurations.DOCUMENT_DATABASE.get(), "library");
    }

    @Inject
    private DocumentTemplate template;

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
    void shouldDeleteById() {
        Book book = new Book(randomUUID().toString(), "Effective Java", 1);
        assertThat(template.insert(book))
                .isNotNull()
                .isEqualTo(book);

        template.delete(Book.class, book.id());
        assertThat(template.find(Book.class, book.id()))
                .isNotNull().isEmpty();
    }

    @Test
    void shouldDeleteAll(){
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
    void shouldUpdateEmbeddable() {
        var workflowStep = WorkflowStep.builder()
                .id("id")
                .key("key")
                .workflowSchemaKey("workflowSchemaKey")
                .stepName("stepName")
                .mainStepType(MainStepType.MAIN)
                .stepNo(1)
                .componentConfigurationKey("componentConfigurationKey")
                .relationTypeKey("relationTypeKey")
                .availableTransitions(List.of(new Transition("TEST_WORKFLOW_STEP_KEY", REPEAT,
                        null, List.of("ADMIN"))))
                .build();
        var result = this.template.insert(workflowStep);

        SoftAssertions.assertSoftly(soft ->{
            soft.assertThat(result).isNotNull();
            soft.assertThat(result.id()).isEqualTo("id");
            soft.assertThat(result.key()).isEqualTo("key");
            soft.assertThat(result.workflowSchemaKey()).isEqualTo("workflowSchemaKey");
            soft.assertThat(result.stepName()).isEqualTo("stepName");
            soft.assertThat(result.mainStepType()).isEqualTo(MainStepType.MAIN);
            soft.assertThat(result.stepNo()).isEqualTo(1);
            soft.assertThat(result.componentConfigurationKey()).isEqualTo("componentConfigurationKey");
            soft.assertThat(result.relationTypeKey()).isEqualTo("relationTypeKey");
            soft.assertThat(result.availableTransitions()).hasSize(1);
            soft.assertThat(result.availableTransitions().get(0).targetWorkflowStepKey()).isEqualTo("TEST_WORKFLOW_STEP_KEY");
            soft.assertThat(result.availableTransitions().get(0).stepTransitionReason()).isEqualTo(REPEAT);
            soft.assertThat(result.availableTransitions().get(0).mailTemplateKey()).isNull();
            soft.assertThat(result.availableTransitions().get(0).restrictedRoleGroups()).hasSize(1);
            soft.assertThat(result.availableTransitions().get(0).restrictedRoleGroups().get(0)).isEqualTo("ADMIN");
        });
    }
}
