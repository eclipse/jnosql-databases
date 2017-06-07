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
import org.jnosql.diana.api.key.BucketManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * The couchbase implementation of {@link BucketManagerFactory}. That has support to
 * {@link BucketManagerFactory#getBucketManager(String)} and also the structure {@link Map}, {@link Set},
 * {@link Queue}, {@link List}. Each structure has this specific implementation.
 * <p>{@link CouchbaseList}</p>
 * <p>{@link CouchbaseSet}</p>
 * <p>{@link CouchbaseQueue}</p>
 * <p>{@link CouchbaseMap}</p>
 */
public class CouchbaseBucketManagerFactory implements BucketManagerFactory<CouchbaseBucketManager> {

    private static final String DEFAULT_BUCKET = "default";

    private final CouchbaseCluster couchbaseCluster;

    private final String user;

    private final String password;

    CouchbaseBucketManagerFactory(CouchbaseCluster couchbaseCluster, String user, String password) {
        this.couchbaseCluster = couchbaseCluster;
        this.user = user;
        this.password = password;
    }


    @Override
    public CouchbaseBucketManager getBucketManager(String bucketName) throws UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucket is required");
        return new CouchbaseBucketManager(getBucket(bucketName), bucketName);
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) throws
            UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucketName is required");
        Objects.requireNonNull(valueValue, "valueValue is required");
        Objects.requireNonNull(keyValue, "keyValue is required");
        return new CouchbaseMap<>(getBucket(bucketName), bucketName, keyValue, valueValue);
    }

    @Override
    public Queue getQueue(String bucketName, Class clazz) throws UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucketName is required");
        Objects.requireNonNull(clazz, "valueValue is required");
        return new CouchbaseQueue<>(getBucket(bucketName), bucketName, clazz);
    }

    @Override
    public Set getSet(String bucketName, Class clazz) throws UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucketName is required");
        Objects.requireNonNull(clazz, "valueValue is required");
        return new CouchbaseSet<>(getBucket(bucketName), bucketName, clazz);
    }

    @Override
    public List getList(String bucketName, Class clazz) throws UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucketName is required");
        Objects.requireNonNull(clazz, "valueValue is required");
        return new CouchbaseList<>(getBucket(bucketName), bucketName, clazz);
    }

    private Bucket getBucket(String bucketName) {
        Objects.requireNonNull(bucketName, "bucket is required");

        /*
        ClusterManager clusterManager = couchbaseCluster.clusterManager(user, password);

        if(!clusterManager.hasBucket(bucketName)){
            BucketSettings settings = DefaultBucketSettings.builder().name(bucketName);
            clusterManager.insertBucket(settings);
        }*/
        if (DEFAULT_BUCKET.equals(bucketName)) {
            return couchbaseCluster.openBucket(bucketName);
        }
        return couchbaseCluster.openBucket(bucketName, password);
    }

    @Override
    public void close() {
        couchbaseCluster.clusterManager();
    }
}
