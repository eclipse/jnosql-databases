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
package org.jnosql.diana.memcached.key;

import com.hazelcast.core.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.KeyValueEntity;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The default implementation of memcached bucket manager
 */
class DefaultHazelcastBucketManager implements HazelcastBucketManager {

    private final IMap map;

    DefaultHazelcastBucketManager(IMap map) {
        this.map = map;
    }

    @Override
    public <K, V> void put(K key, V value) {
        map.put(key, value);
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        map.put(entity.getKey(), entity.getValue().get());
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) {
        map.put(entity.getKey(), entity.getValue().get(), ttl.toMillis(), TimeUnit.MILLISECONDS);
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
        Object value = map.get(key);
        if (value == null) {
            return Optional.empty();
        }
        return Optional.ofNullable(Value.of(value));
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return StreamSupport.stream(keys.spliterator(), false).map((Function<K, Object>) map::get).filter(Objects::nonNull)
                .map(Value::of).collect(toList());
    }

    @Override
    public <K> void remove(K key) {
        map.remove(key);
    }

    @Override
    public <K> void remove(Iterable<K> keys) {
        StreamSupport.stream(keys.spliterator(), false).forEach(this::remove);
    }

    @Override
    public void close() {
    }

    @Override
    public Collection<Value> sql(String query) throws NullPointerException {
        requireNonNull(query, "sql is required");
        return sql(new SqlPredicate(query));
    }

    @Override
    public Collection<Value> sql(String query, Map<String, Object> params) throws NullPointerException {
        requireNonNull(query, "sql is required");
        requireNonNull(params, "params is required");
        final StringBuilder finalQuery = new StringBuilder(query);
        final Consumer<Map.Entry<String, Object>> consumer = e -> {
            String key = ":" + e.getKey();
            int indexOf = query.indexOf(key);
            finalQuery.replace(indexOf, indexOf + key.length(), e.getValue().toString());
        };
        params.entrySet().forEach(consumer);
        return sql(new SqlPredicate(finalQuery.toString()));
    }

    @Override
    public <K, V> Collection<Value> sql(Predicate<K, V> predicate) throws NullPointerException {
        requireNonNull(predicate, "predicate is required");
        Collection<V> values = map.values(predicate);
        return values.stream().map(Value::of).collect(toList());
    }
}
