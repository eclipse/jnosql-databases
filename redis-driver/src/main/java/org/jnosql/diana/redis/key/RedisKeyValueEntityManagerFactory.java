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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.jnosql.diana.api.key.BucketManagerFactory;

import redis.clients.jedis.JedisPool;

/**
 * The redis implementation to {@link BucketManagerFactory} where returns {@link RedisKeyValueEntityManager}
 */
public class RedisKeyValueEntityManagerFactory implements BucketManagerFactory<RedisKeyValueEntityManager> {

    private static final Jsonb PROVDER = JsonbBuilder.create();

    private final JedisPool jedisPool;

    RedisKeyValueEntityManagerFactory(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }


    @Override
    public RedisKeyValueEntityManager getBucketManager(String bucketName) {
        Objects.requireNonNull(bucketName, "bucket name is required");

        return new RedisKeyValueEntityManager(bucketName, PROVDER, jedisPool.getResource());
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        Objects.requireNonNull(bucketName, "bucket name is required");
        Objects.requireNonNull(clazz, "Class type is required");
        return new RedisList<T>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        Objects.requireNonNull(bucketName, "bucket name is required");
        Objects.requireNonNull(clazz, "Class type is required");
        return new RedisSet<T>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        Objects.requireNonNull(bucketName, "bucket name is required");
        Objects.requireNonNull(clazz, "Class type is required");
        return new RedisQueue<T>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        Objects.requireNonNull(bucketName, "bucket name is required");
        Objects.requireNonNull(valueValue, "Class type is required");
        return new RedisMap<>(jedisPool.getResource(), keyValue, valueValue, bucketName);
    }

    /**
     * Creates a {@link SortedSet} from key
     *
     * @param key the key
     * @return the SortedSet from key
     * @throws NullPointerException when key is null
     */
    public SortedSet getSortedSet(String key) throws NullPointerException {
        Objects.requireNonNull(key, "key is required");
        return new DefaultSortedSet(jedisPool.getResource(), key);
    }

    /**
     * Creates {@link Counter}
     *
     * @param key the key to counter
     * @return a counter instance from key
     * @throws NullPointerException when key is null
     */
    public Counter getCounter(String key) throws NullPointerException {
        Objects.requireNonNull(key, "key is required");
        return new DefaultCounter(key, jedisPool.getResource());
    }


    @Override
    public void close() {
        jedisPool.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RedisKeyValueEntityManagerFactory{");
        sb.append("jedisPool=").append(jedisPool);
        sb.append('}');
        return sb.toString();
    }
}
