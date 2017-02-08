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
package org.jnosql.diana.riak.key;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.query.Namespace;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.driver.value.JSONValueProvider;
import org.jnosql.diana.driver.value.JSONValueProviderService;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * The riak implementation to {@link BucketManagerFactory} that returns {@link RiakKeyValueEntityManager}
 * This implementation just has support to {@link RiakKeyValueEntityManagerFactory#getBucketManager(String)}
 * So, these metdhos will returns {@link UnsupportedOperationException}
 * <p>{@link BucketManagerFactory#getList(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getSet(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getQueue(String, Class)}</p>
 * <p>{@link BucketManagerFactory#getMap(String, Class, Class)}</p>
 */
public class RiakKeyValueEntityManagerFactory implements BucketManagerFactory<RiakKeyValueEntityManager> {

    private static final JSONValueProvider PROVDER = JSONValueProviderService.getProvider();
    private final RiakCluster cluster;

    RiakKeyValueEntityManagerFactory(RiakCluster cluster) {
        this.cluster = cluster;
    }

    @Override
    public RiakKeyValueEntityManager getBucketManager(String bucketName) throws UnsupportedOperationException {

        cluster.start();
        RiakClient riakClient = new RiakClient(cluster);
        Namespace quotesBucket = new Namespace(bucketName);

        return new RiakKeyValueEntityManager(riakClient, PROVDER, quotesBucket);
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The riak does not support getList method");
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The riak does not support getSet method");
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The riak does not support getQueue method");
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue)
            throws UnsupportedOperationException {
        throw new UnsupportedOperationException("The riak does not support getMap method");
    }

    @Override
    public void close() {
        cluster.shutdown();
    }

}
