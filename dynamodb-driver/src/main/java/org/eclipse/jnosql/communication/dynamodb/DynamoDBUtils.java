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
package org.eclipse.jnosql.communication.dynamodb;


import jakarta.nosql.keyvalue.KeyValueEntity;
import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import javax.json.bind.Jsonb;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.eclipse.jnosql.communication.dynamodb.ConfigurationAmazonEntity.KEY;
import static org.eclipse.jnosql.communication.dynamodb.ConfigurationAmazonEntity.VALUE;

public class DynamoDBUtils {

    private static final AttributeValue.Builder attributeValueBuilder = AttributeValue.builder();
    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();

    private DynamoDBUtils() {
    }

    public static <K, V> Map<String, AttributeValue> createAttributeValues(K key, V value) {

        Map<String, AttributeValue> createAttributeValues = createAttributeValues(key);
        String valueAsJson = JSONB.toJson(value);

        AttributeValue valueAttributeValue = attributeValueBuilder.s(valueAsJson).build();
        createAttributeValues.put(VALUE, valueAttributeValue);
        return createAttributeValues;
    }

    public static <K, V> Map<String, AttributeValue> createAttributeValues(K key) {

        Map<String, AttributeValue> map = new HashMap<>();
        AttributeValue keyAttributeValue = attributeValueBuilder.s(key.toString()).build();
        map.put(KEY, keyAttributeValue);

        return map;
    }

    public static <K, V> Map<String, AttributeValue> createAttributeValues(KeyValueEntity entity) {
        return createAttributeValues(entity.getKey(), entity.getValue());
    }

    public static <K> Collection<Map<String, AttributeValue>> createAttributeValues(Iterable<KeyValueEntity> entities) {

        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> createAttributeValues(e))
                .collect(Collectors.toList());
    }

    private static Map<String, List<WriteRequest>> createMapWriteRequest(Map<String, AttributeValue> map) {
        return createMapWriteRequest(Arrays.asList(map));
    }

    private static Map<String, List<WriteRequest>> createMapWriteRequest(Collection<Map<String, AttributeValue>> map) {

        PutRequest.Builder putRequestBuilder = PutRequest.builder();
        WriteRequest.Builder writeRequestBuilder = WriteRequest.builder();

        return map
                .stream()
                .map(m -> putRequestBuilder.item(m).build())
                .map(p -> writeRequestBuilder.putRequest(p).build())
                .collect(Collectors.groupingBy(w -> w.toString(), Collectors.toList()));
    }


    public static <K> Map<String, List<WriteRequest>> createMapWriteRequest(Iterable<KeyValueEntity> entities) {

        Collection<Map<String, AttributeValue>> attributeValues = createAttributeValues(entities);
        return createMapWriteRequest(attributeValues);
    }

    public static <K> Map<String, AttributeValue> create(Iterable<K> keys) {

        Map<String, AttributeValue> map = StreamSupport.stream(keys.spliterator(), false)
                .map(e -> e.toString())
                .collect(Collectors.toMap(Function.identity(), k -> attributeValueBuilder.s(k).build()));

        return Collections.unmodifiableMap(map);
    }

    private static <K> Map<String, KeysAndAttributes> createKeysAndAttribute(Iterable<K> keys) {

        KeysAndAttributes.Builder keysAndAttributesBuilder = KeysAndAttributes.builder();

        return StreamSupport.stream(keys.spliterator(), false)
                .collect(Collectors.toMap
                        (
                                e -> e.toString(),
                                k -> keysAndAttributesBuilder.projectionExpression(KEY).keys(createAttributeValues(k)).build())
                );
    }

    public static <K> BatchGetItemRequest createBatchGetItemRequest(Iterable<K> keys) {
        BatchGetItemRequest.Builder batchGetItemRequestBuilder = BatchGetItemRequest.builder();
        return batchGetItemRequestBuilder.requestItems(createKeysAndAttribute(keys)).build();
    }

    public static <K> GetItemRequest createGetItemRequest(K key, String tableName) {
        GetItemRequest.Builder getItemRequest = GetItemRequest.builder();
        return getItemRequest.tableName(tableName).key(createAttributeValues(key)).build();
    }
}