/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.databases.dynamodb.communication;


import org.assertj.core.api.Assertions;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.List;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.dynamodb.communication.DocumentEntityGenerator.createRandomEntity;
import static org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBTestUtils.CONFIG;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
class DynamoDBDocumentManagerTest {

    private DocumentManager documentManager;

    private DynamoDbClient dynamoDbClient;

    private DynamoDbEnhancedClient dynamoDbEnhancedClient;

    private DynamoDbTable<EnhancedDocument> table;

    private String database;
    private UnaryOperator<String> entityNameResolver;
    private boolean tableWasCreated;

    @BeforeEach
    void setUp() {
        var settings = CONFIG.getSettings();
        database = settings.get(MappingConfigurations.DOCUMENT_DATABASE, String.class).orElseThrow();
        entityNameResolver = entityName -> settings.get(DynamoDBConfigurations.ENTITY_PARTITION_KEY, String.class).orElse(entityName);

        var documentManagerFactory = CONFIG.getDocumentManagerFactory(settings);
        documentManager = documentManagerFactory.apply(database);
        Assertions.assertThat(documentManager).isNotNull();

        dynamoDbClient = CONFIG.getDynamoDbClient(settings);
        dynamoDbEnhancedClient = CONFIG.getDynamoDbEnhancedClient(dynamoDbClient);
        table = dynamoDbEnhancedClient.table("music",
                TableSchema.documentSchemaBuilder()
                        .addIndexPartitionKey(TableMetadata.primaryIndexName(), entityNameResolver.apply("@entity"), AttributeValueType.S)
                        .addIndexSortKey(TableMetadata.primaryIndexName(), "_id", AttributeValueType.S)
                        .attributeConverterProviders(AttributeConverterProvider.defaultProvider())
                        .build());

        try {
            table.describeTable();
        } catch (ResourceNotFoundException ex) {
            table.createTable();
        }
    }


    @AfterEach
    void tearDown() {
        table.deleteTable();
    }

    private void cleanTable() {
        table.deleteTable();
        table.createTable();
    }

    @Test
    void shouldReturnName() {
        Assertions.assertThat(documentManager.name()).isEqualTo(database);
    }

    @Test
    void shouldReturnErrorWhenInsertNull() {
        assertSoftly(softly -> {
            softly.assertThatThrownBy(() -> documentManager.insert((DocumentEntity) null))
                    .as("should return error when insert a null DocumentEntity reference")
                    .isExactlyInstanceOf(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((DocumentEntity) null, Duration.ofSeconds(1)))
                    .as("should return error when insert a null DocumentEntity reference with TTL param")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((DocumentEntity) null, null))
                    .as("should return error when insert a null DocumentEntity reference with nullable TTL param")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert(DocumentEntityGenerator.createRandomEntity(), null))
                    .as("should return error when insert a null DocumentEntity reference with nullable TTL param")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((Iterable<DocumentEntity>) null))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((Iterable<DocumentEntity>) null, Duration.ofSeconds(1)))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference with TTL param")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert(List.of(DocumentEntityGenerator.createRandomEntity()), null))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference with nullable TTL param")
                    .isInstanceOfAny(NullPointerException.class);
        });
    }

    @Test
    void shouldInsert() {

        assertSoftly(softly -> {
            DocumentEntity entity = createRandomEntity();
            var _entityType = entity.name();
            var id = entity.find("_id", String.class).orElseThrow();
            var persistedEntity = documentManager.insert(entity);
            softly.assertThat(persistedEntity)
                    .as("documentManager.insert(DocumentEntity) method should return a non-null persistent DocumentEntity")
                    .isNotNull();

            EnhancedDocument persistedItem = table.getItem(Key.builder().partitionValue(_entityType).sortValue(id).build());

            softly.assertThat(persistedItem).as("should return the item from dynamodb").isNotNull();
        });

        assertSoftly(softly -> {
            var entities = List.of(createRandomEntity(),createRandomEntity(),createRandomEntity());
            Iterable<DocumentEntity> persistedEntities = documentManager.insert(entities);
            softly.assertThat(persistedEntities)
                    .as("documentManager.insert(Iterable<>) should returns the non-null list of DocumentEntity").isNotNull();

            assertThat(persistedEntities)
                    .as("documentmanager.insert(iterable<>) should returns a corresponded list of DocumentEntity")
                    .hasSize(3);

            persistedEntities.forEach(entity -> {
                var _entityType = entity.name();
                var id = entity.find("_id", String.class).orElseThrow();
                EnhancedDocument persistedItem = table.getItem(Key.builder().partitionValue(_entityType).sortValue(id).build());
                softly.assertThat(persistedItem)
                        .as("all items of the list of DocumentEntity should be stored on dynamodb database. the entity %s not found"
                                .formatted(id))
                        .isNotNull();
            });
        });

    }

    @Test
    void shouldInserts() {

        DocumentEntity entity = createRandomEntity();
        var _entityType = entity.name();
        var id = entity.find("_id", String.class).orElseThrow();
        var persistedEntity = documentManager.insert(entity);
        assertSoftly(softly -> {
            softly.assertThat(persistedEntity).as("should return the persistent document entity from documentManager.insert() method").isNotNull();
            EnhancedDocument persistedItem = table.getItem(Key.builder().partitionValue(_entityType).sortValue(id).build());
            softly.assertThat(persistedItem).as("should return the item from dynamodb").isNotNull();
        });


    }


}
