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
package org.jnosql.diana.couchbase.key;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import org.jnosql.diana.couchbase.util.CouchbaseClusterUtil;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation of {@link org.jnosql.diana.api.key.BucketManagerFactory}. That has support to
 * {@link org.jnosql.diana.api.key.BucketManagerFactory#getBucketManager(String)}
 * and also the structure {@link Map}, {@link Set},
 * {@link Queue}, {@link List}. Each structure has this specific implementation.
 * <p>{@link CouchbaseList}</p>
 * <p>{@link CouchbaseSet}</p>
 * <p>{@link CouchbaseQueue}</p>
 * <p>{@link CouchbaseMap}</p>
 */
class DefaultCouchbaseBucketManagerFactory implements CouchbaseBucketManagerFactory {

    private final CouchbaseCluster couchbaseCluster;

    private final String user;

    private final String password;

    DefaultCouchbaseBucketManagerFactory(CouchbaseCluster couchbaseCluster, String user, String password) {
        this.couchbaseCluster = couchbaseCluster;
        this.user = user;
        this.password = password;
    }


    @Override
    public CouchbaseBucketManager getBucketManager(String bucketName) throws UnsupportedOperationException {
        requireNonNull(bucketName, "bucket is required");
        return new CouchbaseBucketManager(getBucket(bucketName), bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(valueValue, "valueValue is required");
        requireNonNull(keyValue, "keyValue is required");
        return new CouchbaseMap<>(getBucket(bucketName), bucketName, keyValue, valueValue);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, String key, Class<K> keyValue, Class<V> valueValue)  {

        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(key, "key is required");
        requireNonNull(valueValue, "valueValue is required");
        requireNonNull(keyValue, "keyValue is required");
        return new CouchbaseMap<>(getBucket(bucketName), key, keyValue, valueValue);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        return new CouchbaseQueue<>(getBucket(bucketName), bucketName, clazz);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        return new CouchbaseSet<>(getBucket(bucketName), bucketName, clazz);
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        return new CouchbaseList<>(getBucket(bucketName), bucketName, clazz);
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, String key, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        requireNonNull(key, "key is required");
        return new CouchbaseQueue<>(getBucket(bucketName), key, clazz);
    }

    @Override
    public <T> Set<T> getSet(String bucketName, String key, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        requireNonNull(key, "key is required");
        return new CouchbaseSet<>(getBucket(key), bucketName, clazz);
    }

    @Override
    public <T> List<T> getList(String bucketName, String key, Class<T> clazz) {
        requireNonNull(bucketName, "bucketName is required");
        requireNonNull(clazz, "valueValue is required");
        requireNonNull(key, "key is required");
        return new CouchbaseList<>(getBucket(bucketName), key, clazz);
    }

    private Bucket getBucket(String bucketName) {
        requireNonNull(bucketName, "bucket is required");
        CouchbaseCluster couchbaseCluster = CouchbaseClusterUtil.getCouchbaseCluster(bucketName, this.couchbaseCluster, user, password);
        return couchbaseCluster.openBucket(bucketName);
    }



    @Override
    public void close() {
        couchbaseCluster.clusterManager();
    }
}
