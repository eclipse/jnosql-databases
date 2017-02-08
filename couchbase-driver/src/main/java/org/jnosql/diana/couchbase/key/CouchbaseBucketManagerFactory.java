/*
 * Copyright 2017 Eclipse Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
