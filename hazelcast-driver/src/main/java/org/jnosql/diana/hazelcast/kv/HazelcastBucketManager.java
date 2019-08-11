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

package org.jnosql.diana.hazelcast.kv;

import com.hazelcast.query.Predicate;
import jakarta.nosql.Value;
import jakarta.nosql.kv.BucketManager;

import java.util.Collection;
import java.util.Map;

/**
 * The hazelcast implementation of {@link BucketManager}
 */
public interface HazelcastBucketManager extends BucketManager {


    /**
     * Executes hazelcast sql
     *
     * @param query the sql
     * @return the result sql
     * @throws NullPointerException when there is null sql
     */
    Collection<Value> sql(String query);

    /**
     * Executes hazelcast sql with named sql.
     * E.g.:  bucketManager.sql("name = :name", singletonMap("name", "Matrix"))
     *
     * @param query  the sql
     * @param params the params to bind
     * @return the result sql
     * @throws NullPointerException when there is null sql
     */
    Collection<Value> sql(String query, Map<String, Object> params);

    /**
     * Executes hazelcast sql
     *
     * @param predicate the hazelcast predicate
     * @param <K>       the key type
     * @param <V>       the value type
     * @return the result sql
     * @throws NullPointerException when there is null predicate
     */
    <K, V> Collection<Value> sql(Predicate<K, V> predicate);

}
