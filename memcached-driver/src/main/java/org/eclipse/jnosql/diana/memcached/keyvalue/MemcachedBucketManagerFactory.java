/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.eclipse.jnosql.diana.memcached.keyvalue;

import jakarta.nosql.keyvalue.BucketManagerFactory;
import net.spy.memcached.MemcachedClient;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

final class MemcachedBucketManagerFactory implements BucketManagerFactory {

    private final MemcachedClient client;

    MemcachedBucketManagerFactory(MemcachedClient client) {
        this.client = client;
    }

    @Override
    public MemcachedBucketManager getBucketManager(String bucketName) {
        Objects.requireNonNull(bucketName, "bucketName is required");
        return new MemcachedBucketManager(client, bucketName);
    }

    @Override
    public void close() {
    }

    @Override
    public Map getMap(String bucketName, Class keyValue, Class valueValue) {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public Queue getQueue(String bucketName, Class clazz) {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public Set getSet(String bucketName, Class clazz) {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public List getList(String bucketName, Class clazz) {
        throw new UnsupportedOperationException("This method is not supported");
    }
}
