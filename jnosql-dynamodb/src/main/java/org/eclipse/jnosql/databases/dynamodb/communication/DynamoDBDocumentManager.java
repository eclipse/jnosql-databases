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
import software.amazon.awssdk.enhanced.dynamodb.AttributeConverterProvider;
import software.amazon.awssdk.enhanced.dynamodb.AttributeValueType;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.TableMetadata;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.document.EnhancedDocument;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.eclipse.jnosql.databases.dynamodb.communication.DocumentEntityConverter.toEnhancedDocument;

public class DynamoDBDocumentManager implements DocumentManager {

    private final String database;

    private final Settings settings;

    private final DynamoDbClient dynamoDbClient;

    private final DynamoDbEnhancedClient dynamoDbEnhancedClient;

    private final UnaryOperator<String> entityNameAttributeNameResolver;

    private final ConcurrentHashMap<String, DynamoDbTable<EnhancedDocument>> tables = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Supplier<String>> ttlAttributeNamesByTable = new ConcurrentHashMap<>();

    public DynamoDBDocumentManager(String database, DynamoDbClient dynamoDbClient, Settings settings) {
        this.settings = settings;
        this.database = database;
        this.dynamoDbClient = dynamoDbClient;
        this.dynamoDbEnhancedClient = DynamoDbEnhancedClient.builder().dynamoDbClient(dynamoDbClient).build();
        this.entityNameAttributeNameResolver = this::resolveEntityNameAttributeName;
    }

    private String resolveEntityNameAttributeName(String entityName) {
        return this.settings.get(DynamoDBConfigurations.ENTITY_PARTITION_KEY, String.class).orElse(entityName);
    }

    public DynamoDbClient getDynamoDbClient() {
        return dynamoDbClient;
    }

    public DynamoDbEnhancedClient getDynamoDbEnhancedClient() {
        return dynamoDbEnhancedClient;
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
        var enhancedDocument = convertToEnhancedDocument(documentEntity);
        getTableFor(documentEntity.name())
                .putItem(enhancedDocument);
        return documentEntity;
    }

    private EnhancedDocument convertToEnhancedDocument(DocumentEntity documentEntity) {
        return toEnhancedDocument(entityNameAttributeNameResolver(), documentEntity);
    }

    private DynamoDbTable<EnhancedDocument> getTableFor(String name) {
        return this.tables.computeIfAbsent(name, this::getOrCreateTable);
    }

    private Supplier<String> getTTLAttributeNameFor(String tableName) {
        return this.ttlAttributeNamesByTable.computeIfAbsent(tableName, this::getTTLAttributeNameSupplierFromTable);
    }

    private Supplier<String> getTTLAttributeNameSupplierFromTable(String tableName) {
        DynamoDbTable<EnhancedDocument> table = this.getOrCreateTable(tableName);
        DescribeTimeToLiveResponse describeTimeToLiveResponse = getDynamoDbClient().describeTimeToLive(DescribeTimeToLiveRequest.builder()
                .tableName(table.tableName()).build());
        if (TimeToLiveStatus.ENABLED.equals(describeTimeToLiveResponse.timeToLiveDescription().timeToLiveStatus())) {
            var ttlAttributeName = describeTimeToLiveResponse.timeToLiveDescription().attributeName();
            return () -> ttlAttributeName;
        }
        return unsupportedTTLSupplierFor(table.tableName());
    }

    private Supplier<String> unsupportedTTLSupplierFor(String tableName) {
        return () -> tableName + " don't support TTL operations. Check if TTL support is enabled for this table.";
    }

    private DynamoDbTable<EnhancedDocument> getOrCreateTable(String nameKey) {
        DynamoDbTable<EnhancedDocument> table = dynamoDbEnhancedClient
                .table(nameKey, TableSchema.documentSchemaBuilder()
                        .addIndexPartitionKey(TableMetadata.primaryIndexName(), getEntityNameAttributeName(), AttributeValueType.S)
                        .addIndexSortKey(TableMetadata.primaryIndexName(), DocumentEntityConverter.ID, AttributeValueType.S)
                        .attributeConverterProviders(AttributeConverterProvider.defaultProvider())
                        .build());
        try {
            table.describeTable();
            return table;
        } catch (ResourceNotFoundException ex) {
            if (shouldCreateTables()) {
                table.createTable();
                return table;
            }
            throw ex;
        }
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
        DynamoDbTable<EnhancedDocument> tableFor = getTableFor(documentEntity.name());
        documentEntity.add(getTTLAttributeNameFor(tableFor.tableName()).get(), Instant.now().plus(ttl).truncatedTo(ChronoUnit.SECONDS));
        var enhancedDocument = convertToEnhancedDocument(documentEntity);
        tableFor.putItem(enhancedDocument);
        return documentEntity;
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
        throw new UnsupportedOperationException("count method must be implemented!");
    }

    @Override
    public void close() {
        this.dynamoDbClient.close();
    }
}
