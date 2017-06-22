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

package org.jnosql.diana.redis.key;


import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.redis.key.RedisUtils.createKeyWithNameSpace;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import javax.json.bind.Jsonb;

import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.jnosql.diana.driver.ValueJSON;

import redis.clients.jedis.Jedis;

/**
 * The redis implementation to {@link BucketManager}
 */
public class RedisKeyValueEntityManager implements BucketManager {

    private final String nameSpace;
    private final Jsonb jsonB;

    private final Jedis jedis;

    RedisKeyValueEntityManager(String nameSpace, Jsonb provider, Jedis jedis) {
        this.nameSpace = nameSpace;
        this.jsonB = provider;
        this.jedis = jedis;
    }

    @Override
    public <K, V> void put(K key, V value) throws NullPointerException {
        Objects.requireNonNull(value, "Value is required");
        Objects.requireNonNull(key, "key is required");
        String valideKey = createKeyWithNameSpace(key.toString(), nameSpace);
        jedis.set(valideKey, jsonB.toJson(value));
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity) throws NullPointerException {
        put(entity.getKey(), entity.getValue().get());
    }

    @Override
    public <K> void put(KeyValueEntity<K> entity, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        put(entity);
        String valideKey = createKeyWithNameSpace(entity.getKey().toString(), nameSpace);
        jedis.expire(valideKey, (int) ttl.getSeconds());
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities) throws NullPointerException {
        StreamSupport.stream(entities.spliterator(), false).forEach(this::put);
    }

    @Override
    public <K> void put(Iterable<KeyValueEntity<K>> entities, Duration ttl) throws NullPointerException, UnsupportedOperationException {
        StreamSupport.stream(entities.spliterator(), false).forEach(this::put);
        StreamSupport.stream(entities.spliterator(), false).map(KeyValueEntity::getKey)
                .map(k -> createKeyWithNameSpace(k.toString(), nameSpace))
                .forEach(k -> jedis.expire(k, (int) ttl.getSeconds()));
    }

    @Override
    public <K> Optional<Value> get(K key) throws NullPointerException {
        String value = jedis.get(createKeyWithNameSpace(key.toString(), nameSpace));
        if (value != null && !value.isEmpty()) {
            return Optional.of(ValueJSON.of(value));
        }
        return Optional.empty();
    }

    @Override
    public <K> Iterable<Value> get(Iterable<K> keys) throws NullPointerException {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> jedis.get(createKeyWithNameSpace(k.toString(), nameSpace)))
                .filter(value -> value != null && !value.isEmpty())
                .map(v -> ValueJSON.of(v)).collect(toList());
    }

    @Override
    public <K> void remove(K key) {
        jedis.del(createKeyWithNameSpace(key.toString(), nameSpace));
    }

    @Override
    public <K> void remove(Iterable<K> keys) {
        StreamSupport.stream(keys.spliterator(), false).forEach(this::remove);
    }

    @Override
    public void close() {
        jedis.close();
    }
}
