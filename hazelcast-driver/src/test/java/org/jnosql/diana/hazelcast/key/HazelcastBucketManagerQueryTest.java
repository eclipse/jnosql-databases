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
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.hazelcast.key.model.Movie;
import org.jnosql.diana.hazelcast.key.util.KeyValueEntityManagerFactoryUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static org.junit.Assert.assertEquals;

public class HazelcastBucketManagerQueryTest {

    private HazelcastBucketManager bucketManager;

    private BucketManagerFactory keyValueEntityManagerFactory;



    @Before
    public void init() {
        keyValueEntityManagerFactory = KeyValueEntityManagerFactoryUtils.get();
        bucketManager = (HazelcastBucketManager) keyValueEntityManagerFactory.getBucketManager("movies-entity");

        bucketManager.put("matrix", new Movie("Matrix", 1999, false));
        bucketManager.put("star_wars", new Movie("Star Wars: The Last Jedi", 2017, true));
        bucketManager.put("grease", new Movie("Grease", 1978, false));
        bucketManager.put("justice_league", new Movie("Justice league", 2017, true));
        bucketManager.put("avengers", new Movie("The Avengers", 2012, false));
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnWhenStringQueryIsNull() {
        bucketManager.query((String) null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnWhenPredicateQueryIsNull() {
        bucketManager.query((Predicate<? extends Object, ? extends Object>) null);
    }

    @Test
    public void shouldReturnActive() {
        Collection<Value> result = bucketManager.query("active");
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnActiveAndGreaterThan2000() {
        Collection<Value> result = bucketManager.query("NOT active AND year > 1990");
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnEqualsMatrix() {
        Collection<Value> result = bucketManager.query("name = Matrix");
        assertEquals(1, result.size());
    }


    @Test
    public void shouldReturnActivePredicate() {
        Collection<Value> result = bucketManager.query(equal("active", true));
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnActiveAndGreaterThan2000Predicate() {
        Predicate predicate = and(equal("active", false), greaterEqual("year", 1990));
        Collection<Value> result = bucketManager.query(predicate);
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnEqualsMatrixPredicate() {
        Predicate predicate = equal("name", "Matrix");
        Collection<Value> result = bucketManager.query(predicate);
        assertEquals(1, result.size());
    }

}
