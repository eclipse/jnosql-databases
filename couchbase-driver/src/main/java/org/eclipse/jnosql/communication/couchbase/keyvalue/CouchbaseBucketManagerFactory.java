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
package org.eclipse.jnosql.communication.couchbase.keyvalue;

import jakarta.nosql.keyvalue.BucketManagerFactory;

import java.util.List;
import java.util.Map;
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
 * The default implementation creates the particular structure with the bucket name as the key.
 */
public interface CouchbaseBucketManagerFactory extends BucketManagerFactory {

    /**
     * Creates a {@link Queue} from bucket name
     *
     * @param bucketName a bucket name
     * @param clazz      the value class
     * @param key        key to the queue
     * @param <T>        the value type
     * @return a {@link Queue} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when either bucketName or class are null
     */
    <T> Queue<T> getQueue(String bucketName, String key, Class<T> clazz);

    /**
     * Creates a {@link Set} from bucket name
     *
     * @param bucketName a bucket name
     * @param clazz      the valeu class
     * @param key        key to the set
     * @param <T>        the value type
     * @return a {@link Set} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when either bucketName or class are null
     */
    <T> Set<T> getSet(String bucketName, String key, Class<T> clazz);


    /**
     * Creates a {@link List} from bucket name
     *
     * @param bucketName a bucket name
     * @param clazz      the valeu class
     * @param key        key to the List
     * @param <T>        the value type
     * @return a {@link List} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when either bucketName or class are null
     */
    <T> List<T> getList(String bucketName, String key, Class<T> clazz);


    /**
     * Creates a {@link  Map} from bucket name
     *
     * @param bucketName the bucket name
     * @param key        key to the Map
     * @param keyValue   the key class
     * @param valueValue the value class
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a {@link Map} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when either bucketName or class are null
     */
    <K, V> Map<K, V> getMap(String bucketName, String key, Class<K> keyValue, Class<V> valueValue);
}
