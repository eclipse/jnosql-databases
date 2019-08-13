/*
 *  Copyright (c) 2017 Otávio Santana and others
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
package org.jnosql.diana.couchbase.keyvalue;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonObject;
import jakarta.nosql.Value;
import org.jnosql.diana.driver.JsonbSupplier;

import javax.json.bind.Jsonb;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.stream.Collectors.toSet;


/**
 * The couchbase implementation to {@link Map}
 * that avoid null items, so if any null object will launch {@link NullPointerException}.
 * This class is a wrapper to {@link com.couchbase.client.java.datastructures.collections.CouchbaseMap}. Once they only can save primitive type,
 * objects are converted to Json {@link String} using {@link Jsonb#toJson(Object)} to value and on the key
 * will be used a string representation of the object, Object.toString()
 *
 * @param <V> the object to be stored as value.
 * @param <K> the object to be stored as key.
 */
class CouchbaseMap<K, V> implements Map<K, V> {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();


    private final String bucketName;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final com.couchbase.client.java.datastructures.collections.CouchbaseMap<JsonObject> map;

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
        JsonObject json = map.get(key.toString());
        if (Objects.nonNull(json)) {
            return JSONB.fromJson(json.toString(), valueClass);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key is required");
        Objects.requireNonNull(value, "value is required");
        JsonObject json = map.put(key.toString(), JsonObjectCouchbaseUtil.toJson(JSONB, value));
        if (Objects.nonNull(json)) {
            return JSONB.fromJson(json.toString(), valueClass);
        }
        return null;
    }

    @Override
    public V remove(Object key) {
        Objects.requireNonNull(key, "key is required");
        JsonObject json = map.remove(key.toString());
        if (Objects.nonNull(json)) {
            return JSONB.fromJson(json.toString(), valueClass);
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
        return values().stream().anyMatch(value::equals);
    }

    @Override
    public Set<K> keySet() {
        return map.keySet().stream()
                .map(Value::of).map(v -> v.get(keyClass))
                .collect(toSet());
    }

    @Override
    public Collection<V> values() {

        Collection<V> values = new ArrayList<>();

        for (Object object : map.values()) {
            values.add(convertValue(object));
        }
        return values;
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        Map<K, V> copy = new HashMap<>();

        for (Entry<String, JsonObject> entry : map.entrySet()) {
            String key = entry.getKey();
            V value = convertValue(entry.getValue());
            copy.put((K) key, value);
        }
        return copy.entrySet();
    }

    private V convertValue(Object value) {
        if(value instanceof Map) {
            return JSONB.fromJson(JsonObject.from(Map.class.cast(value)).toString(), valueClass);
        } else if(value instanceof JsonObject) {
            return JSONB.fromJson(JsonObject.class.cast(value).toString(), valueClass);
        }

        throw new IllegalStateException("Couchbase does not support the structure value " + value.getClass().getName());
    }
}
