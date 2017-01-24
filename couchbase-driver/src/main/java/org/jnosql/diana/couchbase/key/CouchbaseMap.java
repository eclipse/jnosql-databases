/*
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

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;


class CouchbaseMap<K, V> implements Map<K, V> {

    private final Bucket bucket;

    private final String bucketName;
    private final Class<V> clazz;

    CouchbaseMap(Bucket bucket, String bucketName, Class<V> clazz) {
        this.bucket = bucket;
        this.bucketName = bucketName;
        this.clazz = clazz;
    }

    @Override
    public int size() {
        return bucket.mapSize(bucketName);
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public V get(Object key) {
        Objects.requireNonNull(key, "key is required");
        return bucket.mapGet(bucketName, key.toString(), clazz);
    }

    @Override
    public V put(K key, V value) {
        Objects.requireNonNull(key, "key is required");
        Objects.requireNonNull(value, "value is required");
        bucket.mapAdd(bucketName, Value.of(key).get(String.class), value);
        return value;
    }

    @Override
    public V remove(Object key) {
        Objects.requireNonNull(key, "key is required");
        bucket.mapRemove(bucketName, Value.of(key).get(String.class));
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
        bucket.remove(bucketName);
    }

    @Override
    public boolean containsKey(Object o) {
        throw new UnsupportedOperationException("The couchbae does not support containsKey method");
    }

    @Override
    public boolean containsValue(Object o) {
        throw new UnsupportedOperationException("The couchbae does not support containsValue(Object o) method");
    }

    @Override
    public Set<K> keySet() {
        throw new UnsupportedOperationException("Couchbase does not support keySet() method");
    }

    @Override
    public Collection<V> values() {
        throw new UnsupportedOperationException("Couchbase does not support values() method");
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        throw new UnsupportedOperationException("Couchbase does not support entrySet() method");
    }
}
