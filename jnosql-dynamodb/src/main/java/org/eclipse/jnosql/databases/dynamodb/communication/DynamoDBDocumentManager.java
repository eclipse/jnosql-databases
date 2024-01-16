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

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.databases.dynamodb.communication.DocumentEntityConverter.toItem;

public class DynamoDBDocumentManager implements DocumentManager {

    private final String database;

    private final Settings settings;

    private final DynamoDbClient dynamoDbClient;

    private final UnaryOperator<String> entityNameAttributeNameResolver;

    private final ConcurrentHashMap<String, Supplier<String>> ttlAttributeNamesByTable = new ConcurrentHashMap<>();

    public DynamoDBDocumentManager(String database, DynamoDbClient dynamoDbClient, Settings settings) {
        this.settings = settings;
        this.database = database;
        this.dynamoDbClient = dynamoDbClient;
        this.entityNameAttributeNameResolver = this::resolveEntityNameAttributeName;
    }

    private String resolveEntityNameAttributeName(String entityName) {
        return this.settings.get(DynamoDBConfigurations.ENTITY_PARTITION_KEY, String.class).orElse(entityName);
    }

    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }

    @Override
    public String name() {
        return database;
    }

    UnaryOperator<String> entityNameAttributeNameResolver() {
        return this.entityNameAttributeNameResolver;
    }

    @Override
    public DocumentEntity insert(DocumentEntity documentEntity) {
        requireNonNull(documentEntity, "documentEntity is required");
        getDynamoDbClient().putItem(PutItemRequest.builder()
                .tableName(createIfNeeded(documentEntity.name()).table().tableName())
                .item(asItem(documentEntity))
                .build());
        return documentEntity;
    }

    private Map<String, AttributeValue> asItem(DocumentEntity documentEntity) {
        return toItem(entityNameAttributeNameResolver(), documentEntity);
    }

    private Supplier<String> getTTLAttributeNameFor(String tableName) {
        return this.ttlAttributeNamesByTable.computeIfAbsent(tableName, this::getTTLAttributeNameSupplierFromTable);
    }

    private Supplier<String> getTTLAttributeNameSupplierFromTable(String tableName) {
        createIfNeeded(tableName);
        DescribeTimeToLiveResponse describeTimeToLiveResponse = getDynamoDbClient().describeTimeToLive(DescribeTimeToLiveRequest.builder()
                .tableName(tableName).build());
        if (TimeToLiveStatus.ENABLED.equals(describeTimeToLiveResponse.timeToLiveDescription().timeToLiveStatus())) {
            var ttlAttributeName = describeTimeToLiveResponse.timeToLiveDescription().attributeName();
            return () -> ttlAttributeName;
        }
        return unsupportedTTLSupplierFor(tableName);
    }

    private Supplier<String> unsupportedTTLSupplierFor(String tableName) {
        return () -> tableName + " don't support TTL operations. Check if TTL support is enabled for this table.";
    }

    private DescribeTableResponse createIfNeeded(String tableName) {
        if (shouldCreateTables()) {
            try {
                return getDynamoDbClient().describeTable(DescribeTableRequest.builder()
                        .tableName(tableName)
                        .build());
            } catch (ResourceNotFoundException ex) {
                return createTable(tableName);
            }
        }
        return getDynamoDbClient().describeTable(DescribeTableRequest.builder()
                .tableName(tableName)
                .build());
    }

    private DescribeTableResponse createTable(String tableName) {
        try (var waiter = getDynamoDbClient().waiter()) {
            getDynamoDbClient().createTable(CreateTableRequest.builder()
                    .tableName(tableName)
                    .keySchema(defaultKeySchemaFor(tableName))
                    .attributeDefinitions(defaultAttributeDefinitionsFor(tableName))
                    .provisionedThroughput(defaultProvisionedThroughputFor(tableName))
                    .streamSpecification(defaultStreamSpecificationFor(tableName))
                    .build());

            var tableRequest = DescribeTableRequest.builder().tableName(tableName).build();
            var waiterResponse = waiter.waitUntilTableExists(tableRequest);
            return waiterResponse.matched().response().orElseThrow();
        }
    }

    private StreamSpecification defaultStreamSpecificationFor(String tableName) {
        return null;
    }

    private ProvisionedThroughput defaultProvisionedThroughputFor(String tableName) {
        return DynamoTableUtils.createProvisionedThroughput(null, null);
    }

    private Collection<AttributeDefinition> defaultAttributeDefinitionsFor(String tableName) {
        return List.of(
                AttributeDefinition.builder().attributeName(getEntityNameAttributeName()).attributeType(ScalarAttributeType.S).build(),
                AttributeDefinition.builder().attributeName(DocumentEntityConverter.ID).attributeType(ScalarAttributeType.S).build()
        );
    }

    private Collection<KeySchemaElement> defaultKeySchemaFor(String tableName) {
        return List.of(
                KeySchemaElement.builder().attributeName(getEntityNameAttributeName()).keyType(KeyType.HASH).build(),
                KeySchemaElement.builder().attributeName(DocumentEntityConverter.ID).keyType(KeyType.RANGE).build()
        );
    }

    private boolean shouldCreateTables() {
        return this.settings
                .get(DynamoDBConfigurations.CREATE_TABLES, Boolean.class)
                .orElse(false);
    }

    private String getEntityNameAttributeName() {
        return entityNameAttributeNameResolver().apply(DocumentEntityConverter.ENTITY);
    }

    @Override
    public DocumentEntity insert(DocumentEntity documentEntity, Duration ttl) {
        requireNonNull(documentEntity, "documentEntity is required");
        requireNonNull(ttl, "ttl is required");
        documentEntity.add(getTTLAttributeNameFor(documentEntity.name()).get(), Instant.now().plus(ttl).truncatedTo(ChronoUnit.SECONDS));
        return insert(documentEntity);
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .toList();
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        requireNonNull(entities, "entities is required");
        requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> this.insert(e, ttl))
                .toList();
    }

    @Override
    public DocumentEntity update(DocumentEntity documentEntity) {
        throw new UnsupportedOperationException("update method must be implemented!");
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .toList();
    }

    @Override
    public void delete(DocumentDeleteQuery documentDeleteQuery) {
        throw new UnsupportedOperationException("delete method must be implemented!");
    }

    @Override
    public Stream<DocumentEntity> select(DocumentQuery documentQuery) {
        throw new UnsupportedOperationException("select method must be implemented!");
    }

    @Override
    public long count(String tableName) {
        Objects.requireNonNull(tableName, "tableName is required");
        try {
            return getDynamoDbClient()
                    .describeTable(DescribeTableRequest.builder().tableName(tableName).build())
                    .table()
                    .itemCount();
        } catch (ResourceNotFoundException ex) {
            return 0;
        }
    }

    @Override
    public void close() {
        this.dynamoDbClient.close();
    }
}
