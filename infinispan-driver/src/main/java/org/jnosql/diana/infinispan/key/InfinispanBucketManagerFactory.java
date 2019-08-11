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
 *   The Infinispan Team
 */

package org.jnosql.diana.infinispan.key;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import org.infinispan.commons.api.BasicCacheContainer;
import jakarta.nosql.kv.BucketManagerFactory;

/**
 * The Infinispan implementation of {@link BucketManagerFactory}
 */
public class InfinispanBucketManagerFactory implements BucketManagerFactory {

    private final BasicCacheContainer cacheContainer;

    InfinispanBucketManagerFactory(BasicCacheContainer cacheContainer) {
        this.cacheContainer = cacheContainer;
    }

    @Override
    public InfinispanBucketManager getBucketManager(String bucketName) {
        return new InfinispanBucketManager(cacheContainer.getCache(bucketName));
    }

    @Override
    public <T> List<T> getList(String bucketName, Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Set<T> getSet(String bucketName, Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> Queue<T> getQueue(String bucketName, Class<T> clazz) {
        throw new UnsupportedOperationException();
    }

    @Override
    public <K, V> Map<K, V> getMap(String bucketName, Class<K> keyValue, Class<V> valueValue) {
        return cacheContainer.getCache(bucketName);
    }

    @Override
    public void close() {

    }
}
