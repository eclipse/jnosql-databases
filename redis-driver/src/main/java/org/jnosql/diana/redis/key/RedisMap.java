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

<<<<<<< HEAD
import static java.util.Objects.requireNonNull;
=======
import redis.clients.jedis.Jedis;
>>>>>>> 3169492abbc79dce61fa385e55d29f4204b22509

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.jnosql.diana.driver.value.JSONValueProvider;

<<<<<<< HEAD
import redis.clients.jedis.Jedis;
=======
import static java.util.Objects.requireNonNull;
>>>>>>> 3169492abbc79dce61fa385e55d29f4204b22509

public class RedisMap<K, V> implements Map<K, V> {


    private final Class<K> keyClass;

    private final Class<V> valueClass;

    private final String nameSpace;

    private final Jedis jedis;

    private final JSONValueProvider provider;


    RedisMap(Jedis jedis, Class<K> keyValue, Class<V> valueClass, String keyWithNameSpace,JSONValueProvider provider) {
        this.keyClass = keyValue;
        this.valueClass = valueClass;
        this.nameSpace = keyWithNameSpace;
        this.jedis = jedis;
        this.provider = provider;
    }

    @Override
    public int size() {
        return jedis.hgetAll(nameSpace).size();
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return jedis.hexists(nameSpace, provider.toJson(requireNonNull(key)));
    }

    @Override
    public boolean containsValue(Object value) {
        requireNonNull(value);
        String valueString = provider.toJson(value);
        Map<String, String> map = createRedisMap();
        return map.containsValue(valueString);
    }

    @Override
    public V get(Object key) {
        requireNonNull(key, "Key is required");
        String value = jedis.hget(nameSpace, provider.toJson(key));
        if (value != null && !value.isEmpty()) {
        	return provider.of(value).get(valueClass);
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        requireNonNull(value, "Value is required");
        requireNonNull(value, "Key is required");
        String keyJson = provider.toJson(key);
        jedis.hset(nameSpace, keyJson, provider.toJson(value));
        return value;
    }

    @Override
    public V remove(Object key) {
        V value = get(key);
        if (value != null) {
            jedis.hdel(nameSpace, provider.toJson(requireNonNull(key, "Key is required")));
            return value;
        }
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        requireNonNull(map, "map is required");

        for (K key : map.keySet()) {
            V value = map.get(key);
            if (value != null) {
                put(key, value);
            }
        }
    }

    @Override
    public void clear() {
        jedis.del(nameSpace);
    }

    @Override
    public Set<K> keySet() {
        return createHashMap().keySet();
    }

    @Override
    public Collection<V> values() {
        return createHashMap().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return createHashMap().entrySet();
    }

    private Map<String, String> createRedisMap() {
        Map<String, String> map = jedis.hgetAll(nameSpace);
        return map;
    }

    private Map<K, V> createHashMap() {
        Map<String, String> redisMap = createRedisMap();
        return redisMap.keySet().stream().collect(Collectors
                .toMap(k -> provider.of(k).get(keyClass), k -> provider.of(redisMap.get(k)).get(valueClass)));
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RedisMap{");
        sb.append("keyClass=").append(keyClass);
        sb.append(", valueClass=").append(valueClass);
        sb.append(", nameSpace='").append(nameSpace).append('\'');
        sb.append(", jedis=").append(jedis);
        sb.append(", provider=").append(provider);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(nameSpace);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (RedisMap.class.isInstance(obj)) {
            RedisMap otherRedis = RedisMap.class.cast(obj);
            return Objects.equals(otherRedis.nameSpace, nameSpace);
        }
        return false;
    }

}
