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
import org.assertj.core.api.SoftAssertions;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.mapping.core.config.MappingConfigurations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.enhanced.dynamodb.model.DescribeTableEnhancedResponse;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBTestUtils.CONFIG;
import static org.eclipse.jnosql.databases.dynamodb.communication.DynamoDBTestUtils.createTable;

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
        var settings = CONFIG.getSettings(CONFIG.getDynamoDBHost("localhost", 8000));
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
            tableWasCreated = true;
        }
    }

    @AfterEach
    void tearDown() {
        if (tableWasCreated)
            table.deleteTable();
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
            softly.assertThatThrownBy(() -> documentManager.insert((Iterable<DocumentEntity>) null))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((Iterable<DocumentEntity>) null, Duration.ofSeconds(1)))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference with TTL param")
                    .isInstanceOfAny(NullPointerException.class);
            softly.assertThatThrownBy(() -> documentManager.insert((Iterable<DocumentEntity>) null, null))
                    .as("should return error when insert a null Iterable<DocumentEntity> reference with nullable TTL param")
                    .isInstanceOfAny(NullPointerException.class);
        });
    }

    @Test
    void shouldInsertDocumentWithNoSubDocuments() {

        DocumentEntity entity = DocumentEntityGenerator.getEntityWithSubDocuments(0);
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
