/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.redis.keyvalue;

import org.eclipse.jnosql.communication.driver.JsonbSupplier;
import redis.clients.jedis.JedisPool;

import javax.json.bind.Jsonb;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

class DefaultRedisBucketManagerFactory implements RedisBucketManagerFactory {

    private static final Jsonb JSON = JsonbSupplier.getInstance().get();

    private final JedisPool jedisPool;

    DefaultRedisBucketManagerFactory(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }


    @Override
    public RedisBucketManager apply(String bucketName) {
        requireNonNull(bucketName, "bucket name is required");

        return new RedisBucketManager(bucketName, JSON, jedisPool.getResource());
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucket name is required");
        requireNonNull(clazz, "Class type is required");
        return new RedisList<>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucket name is required");
        requireNonNull(clazz, "Class type is required");
        return new RedisSet<>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucket name is required");
        requireNonNull(clazz, "Class type is required");
        return new RedisQueue<>(jedisPool.getResource(), clazz, bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        requireNonNull(bucketName, "bucket name is required");
        requireNonNull(valueValue, "Class type is required");
        return new RedisMap<>(jedisPool.getResource(), keyValue, valueValue, bucketName);
    }

    @Override
    public SortedSet getSortedSet(String key) throws NullPointerException {
        requireNonNull(key, "key is required");
        return new DefaultSortedSet(jedisPool.getResource(), key);
    }

    @Override
    public Counter getCounter(String key) throws NullPointerException {
        requireNonNull(key, "key is required");
        return new DefaultCounter(key, jedisPool.getResource());
    }


    @Override
    public void close() {
        jedisPool.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("RedisBucketManagerFactory{");
        sb.append("jedisPool=").append(jedisPool);
        sb.append('}');
        return sb.toString();
    }
}
