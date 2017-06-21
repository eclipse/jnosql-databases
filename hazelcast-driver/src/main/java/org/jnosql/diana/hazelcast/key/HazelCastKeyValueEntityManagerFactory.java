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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import org.jnosql.diana.api.key.BucketManagerFactory;

/**
 * The hazelcast implementation of {@link BucketManagerFactory}
 */
public class HazelCastKeyValueEntityManagerFactory implements BucketManagerFactory<HazelCastKeyValueEntityManager> {

    private final HazelcastInstance hazelcastInstance;

    HazelCastKeyValueEntityManagerFactory(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    @Override
    public HazelCastKeyValueEntityManager getBucketManager(String bucketName) {
        return new HazelCastKeyValueEntityManager(hazelcastInstance.getMap(bucketName));
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        return hazelcastInstance.getList(bucketName);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        return hazelcastInstance.getSet(bucketName);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        return hazelcastInstance.getQueue(bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        return hazelcastInstance.getMap(bucketName);
    }

    @Override
    public void close() {

    }
}
