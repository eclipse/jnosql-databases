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
 *   The Infinispan Team
 */

package org.jnosql.diana.infinispan.key;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.infinispan.commons.api.BasicCache;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;

/**
 * The Infinispan implementation of {@link BucketManager}
 */
public class InfinispanBucketManager implements BucketManager {

    private final BasicCache cache;

    InfinispanBucketManager(BasicCache cache) {
        this.cache = cache;
    }

    @Override
    public <K, V> void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        cache.put(entity.getKey(), entity.getValue().get());
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) {
        cache.put(entity.getKey(), entity.getValue().get(), ttl.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities) throws NullPointerException {
        StreamSupport.stream(entities.spliterator(), false).forEach(this::put);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        StreamSupport.stream(entities.spliterator(), false).forEach(kv -> this.put(kv, ttl));
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {
        Object value = cache.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(Value.of(value));
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return StreamSupport.stream(keys.spliterator(), false).map(k -> cache.get(k)).filter(Objects::nonNull)
                .map(Value::of).collect(Collectors.toList());
    }

    @Override
    public <K> void remove(K key) {
        cache.remove(key);
    }

    @Override
    public <K> void remove(Iterable<K> keys) {
        StreamSupport.stream(keys.spliterator(), false).forEach(this::remove);
    }

    @Override
    public void close() {
    }
}
