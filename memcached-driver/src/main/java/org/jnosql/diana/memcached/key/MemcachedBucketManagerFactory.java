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
package org.jnosql.diana.memcached.key;

import net.spy.memcached.ConnectionFactory;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

final class MemcachedBucketManagerFactory implements BucketManagerFactory {

    private final ConnectionFactory factory;

    private final List<InetSocketAddress> addresses;

    MemcachedBucketManagerFactory(ConnectionFactory factory, List<InetSocketAddress> addresses) {
        this.factory = factory;
        this.addresses = addresses;
    }

    @Override
    public BucketManager getBucketManager(String bucketName) {
        return null;
    }

    @Override
    public void close() {
    }

    @Override
    public Map getMap(String bucketName, Class keyValue, Class valueValue) {
        return null;
    }

    @Override
    public Queue getQueue(String bucketName, Class clazz) {
        return null;
    }

    @Override
    public Set getSet(String bucketName, Class clazz) {
        return null;
    }

    @Override
    public List getList(String bucketName, Class clazz) {
        return null;
    }
}
