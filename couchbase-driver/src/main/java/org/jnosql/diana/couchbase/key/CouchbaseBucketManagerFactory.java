/*
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

import com.couchbase.client.java.CouchbaseCluster;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

/**
 * The couchbase implementation of {@link BucketManagerFactory}. That has support to
 * {@link BucketManagerFactory#getBucketManager(String)}.
 * So, these methods will return an {@link UnsupportedOperationException}:
 * <p>{@link CouchbaseBucketManagerFactory#getMap(String, Class, Class)}</p>
 * <p>{@link CouchbaseBucketManagerFactory#getQueue(String, Class)}</p>
 * <p>{@link CouchbaseBucketManagerFactory#getSet(String, Class)}</p>
 * <p>{@link CouchbaseBucketManagerFactory#getList(String, Class)}</p>
 */
public class CouchbaseBucketManagerFactory implements BucketManagerFactory {

    private final CouchbaseCluster couchbaseCluster;

    private final String user;

    private final String password;

    CouchbaseBucketManagerFactory(CouchbaseCluster couchbaseCluster, String user, String password) {
        this.couchbaseCluster = couchbaseCluster;
        this.user = user;
        this.password = password;
    }


    @Override
    public BucketManager getBucketManager(String bucketName) throws UnsupportedOperationException {
        Objects.requireNonNull(bucketName, "bucket is required");

  /*      ClusterManager clusterManager = couchbaseCluster.clusterManager(user, password);

        if(!clusterManager.hasBucket(bucketName)){
            BucketSettings settings = DefaultBucketSettings.builder().name(bucketName);
            clusterManager.insertBucket(settings);
        }*/
        if ("default".equals(bucketName)) {
            return new CouchbaseBucketManager(couchbaseCluster.openBucket(bucketName), bucketName);
        }
        return new CouchbaseBucketManager(couchbaseCluster.openBucket(bucketName, password), bucketName);
    }

    @Override
    public Map getMap(String bucketName, Class keyValue, Class valueValue) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Couchbase does not support getMap method");
    }

    @Override
    public Queue getQueue(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Couchbase does not support getQueue method");
    }

    @Override
    public Set getSet(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Couchbase does not support getSet method");
    }

    @Override
    public List getList(String bucketName, Class clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("Couchbase does not support getList method");
    }

    @Override
    public void close() {
        couchbaseCluster.clusterManager();
    }
}
