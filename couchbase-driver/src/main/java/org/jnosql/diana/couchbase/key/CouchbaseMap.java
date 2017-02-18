/*
 * Copyright 2017 Otavio Santana and others
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.couchbase.key;

import com.couchbase.client.java.Bucket;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * The couchbase implementation to {@link Map}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link com.couchbase.client.java.datastructures.collections.CouchbaseMap}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link JSONValueProvider#toJson(Object)} to value and on the key
 * will be used a string representation of the object, Object.toString()
 *
 * @param <V> the object to be stored as value.
 * @param <K> the object to be stored as key.
 */
public class CouchbaseMap<K, V> implements Map<K, V> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();


    private final String bucketName;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final com.couchbase.client.java.datastructures.collections.CouchbaseMap<String> map;

    CouchbaseMap(Bucket bucket, String bucketName, Class<K> keyClass, Class<V> valueClass) {
        this.bucketName = bucketName + ":map";
        this.valueClass = valueClass;
        this.keyClass = keyClass;
        map = new com.couchbase.client.java.datastructures.collections.CouchbaseMap<>(this.bucketName, bucket);
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public V get(Object key) {
        Objects.requireNonNull(key, "key is required");
        String json = map.get(key.toString());
        if (Objects.nonNull(json)) {
            return PROVDER.of(json).get(valueClass);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key is required");
        Objects.requireNonNull(value, "value is required");
        String json = map.put(key.toString(), PROVDER.toJson(value));
        if (Objects.nonNull(json)) {
            return PROVDER.of(json).get(valueClass);
        }
        return null;
    }

    @Override
    public V remove(Object key) {
        Objects.requireNonNull(key, "key is required");
        String json = map.remove(key.toString());
        if (Objects.nonNull(json)) {
            return PROVDER.of(json).get(valueClass);
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        Objects.requireNonNull(map, "map is required");
        for (K key : map.keySet()) {
            put(key, map.get(key));
        }
    }

    @Override
    public void clear() {
        map.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        Objects.requireNonNull(key, "key is required");
        return map.containsKey(key.toString());
    }

    @Override
    public boolean containsValue(Object value) {
        Objects.requireNonNull(value, "key is required");
        return map.containsValue(PROVDER.toJson(value));
    }

    @Override
    public Set<K> keySet() {
        return map.keySet().stream().map(Value::of).map(v -> v.get(keyClass)).collect(Collectors.toSet());
    }

    @Override
    public Collection<V> values() {
        return map.values().stream().map(PROVDER::of).map(v -> v.get(valueClass)).collect(Collectors.toList());
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Map<K, V> copy = new HashMap<>();

        for (Entry<String, String> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            copy.put(Value.of(key).get(keyClass), PROVDER.of(value).get(valueClass));
        }
        return copy.entrySet();
    }
}
