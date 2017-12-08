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
package org.jnosql.diana.hazelcast.key;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

class DefaultHazelCastBucketManagerFactory implements HazelCastBucketManagerFactory {

    private final HazelcastInstance hazelcastInstance;

    DefaultHazelCastBucketManagerFactory(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public HazelCastBucketManager getBucketManager(String bucketName) {
        requireNonNull(bucketName, "bucketName is required");
        return new HazelCastBucketManager(hazelcastInstance.getMap(bucketName));
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "clazz is required");
        return hazelcastInstance.getList(bucketName);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "clazz is required");
        return hazelcastInstance.getSet(bucketName);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "clazz is required");
        return hazelcastInstance.getQueue(bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(keyValue, "keyValue is required");
        requireNonNull(valueValue, "valueValue is required");;
        return hazelcastInstance.getMap(bucketName);
    }

    @Override
    public void close() {

    }

    @Override
    public IAtomicLong getAtomicLong(String bucketName) throws NullPointerException {
        requireNonNull(bucketName, "bucketName is required");
        return hazelcastInstance.getAtomicLong(bucketName);
    }
}
