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


import com.hazelcast.core.IAtomicLong;
import jakarta.nosql.key.BucketManagerFactory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * The hazelcast implementation of {@link BucketManagerFactory}
 */
public interface HazelcastBucketManagerFactory extends BucketManagerFactory<HazelcastBucketManager> {

    /**
     * Creates a {@link List} from bucket name
     *
     * @param bucketName a bucket name
     * @param <T>        the value type
     * @return a {@link List} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when the bucke name is null
     */
    <T> List<T> getList(String bucketName) throws UnsupportedOperationException, NullPointerException;

    /**
     * Creates a {@link Set} from bucket name
     *
     * @param bucketName a bucket name
     * @param <T>        the value type
     * @return a {@link Set} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when the bucket name is null
     */
    <T> Set<T> getSet(String bucketName) throws UnsupportedOperationException, NullPointerException;

    /**
     * Creates a {@link Queue} from bucket name
     *
     * @param bucketName a bucket name
     * @param <T>        the value type
     * @return a {@link Queue} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when the bucket name is null
     */
    <T> Queue<T> getQueue(String bucketName) throws UnsupportedOperationException, NullPointerException;

    /**
     * Creates a {@link  Map} from bucket name
     *
     * @param bucketName the bucket name
     * @param <K>        the key type
     * @param <V>        the value type
     * @return a {@link Map} instance
     * @throws UnsupportedOperationException when the database does not have to it
     * @throws NullPointerException          when the bucket name is null
     */
    <K, V> Map<K, V> getMap(String bucketName) throws
            UnsupportedOperationException, NullPointerException;

    /**
     * Creates a {@link IAtomicLong} implementation
     *
     * @param bucketName the bucket name
     * @return a {@link IAtomicLong} instance
     * @throws NullPointerException when bucketName is null
     */
    IAtomicLong getAtomicLong(String bucketName) throws NullPointerException;
}
