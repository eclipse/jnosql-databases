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

import com.hazelcast.query.Predicate;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;

import java.util.Collection;

/**
 * The hazelcast implementation of {@link BucketManager}
 */
public interface HazelcastBucketManager extends BucketManager {


    /**
     * Executes hazelcast query
     *
     * @param query the query
     * @return the result query
     * @throws NullPointerException when there is null query
     */
    Collection<Value> query(String query) throws NullPointerException;

    /**
     * Executes hazelcast query
     *
     * @param predicate the hazelcast predicate
     * @param <K>       the key type
     * @param <V>       the value type
     * @return the result query
     * @throws NullPointerException when there is null predicate
     */
    <K, V> Collection<Value> query(Predicate<K, V> predicate) throws NullPointerException;

}
