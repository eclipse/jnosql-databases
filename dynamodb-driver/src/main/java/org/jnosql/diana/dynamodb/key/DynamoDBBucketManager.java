/*
 *  Copyright (c) 2018 Otávio Santana and others
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
package org.jnosql.diana.dynamodb.key;

import jakarta.nosql.Value;
import jakarta.nosql.key.BucketManager;
import jakarta.nosql.key.KeyValueEntity;
import org.jnosql.diana.driver.ValueJSON;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.jnosql.diana.dynamodb.ConfigurationAmazonEntity.VALUE;
import static org.jnosql.diana.dynamodb.DynamoDBUtils.createAttributeValues;
import static org.jnosql.diana.dynamodb.DynamoDBUtils.createBatchGetItemRequest;
import static org.jnosql.diana.dynamodb.DynamoDBUtils.createGetItemRequest;
import static org.jnosql.diana.dynamodb.DynamoDBUtils.createMapWriteRequest;

public class DynamoDBBucketManager implements BucketManager {


    private DynamoDbClient client;
    private String tableName;
    private static final Function<AttributeValue, String> TO_JSON = e -> e.s();

    public DynamoDBBucketManager(DynamoDbClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {
        client.putItem(PutItemRequest.builder().tableName(tableName).item(createAttributeValues(key, value)).build());
    }

    @Override
    public void put(KeyValueEntity entity) throws NullPointerException {
        put(entity.getKey(), entity.getValue());
    }

    @Override
    public void put(KeyValueEntity entity, Duration ttl)
            throws NullPointerException, UnsupportedOperationException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void put(Iterable<KeyValueEntity> entities) throws NullPointerException {
        client.batchWriteItem(BatchWriteItemRequest.builder().requestItems(createMapWriteRequest(entities)).build());
    }

    @Override
    public  void put(Iterable<KeyValueEntity> entities, Duration ttl) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {

        Objects.requireNonNull(key, "key is required");

        if (key.toString().isEmpty()) {
            throw new IllegalArgumentException("The Key is irregular");
        }

        GetItemResponse getItemResponse = client.getItem(createGetItemRequest(key, tableName));
        Map<String, AttributeValue> item = getItemResponse.item();
        AttributeValue attributeValue = item.get(VALUE);

        return Optional.ofNullable(attributeValue)
                .map(TO_JSON)
                .map(ValueJSON::of);
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {

        return client.batchGetItem(createBatchGetItemRequest(keys))
                .responses()
                .values()
                .stream()
                .flatMap(l -> StreamSupport.stream(l.spliterator(), false))
                .flatMap(v -> v.values().stream())
                .map(TO_JSON)
                .map(ValueJSON::of)
                .collect(Collectors.toList());
    }

    @Override
    public <K> void remove(K key) throws NullPointerException {
        client.deleteItem(DeleteItemRequest.builder().tableName(tableName).key(createAttributeValues(key)).build());
    }

    @Override
    public <K> void remove(Iterable<K> keys) throws NullPointerException {
        keys.forEach(this::remove);
    }

    @Override
    public void close() {
        client.close();
    }
}