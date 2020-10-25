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
package org.eclipse.jnosql.communication.hazelcast.keyvalue;

import com.hazelcast.query.Predicate;
import jakarta.nosql.Value;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.util.KeyValueEntityManagerFactoryUtils;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.model.Movie;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.query.Predicates.and;
import static com.hazelcast.query.Predicates.equal;
import static com.hazelcast.query.Predicates.greaterEqual;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class HazelcastBucketManagerQueryTest {

    private HazelcastBucketManager bucketManager;

    private BucketManagerFactory keyValueEntityManagerFactory;

    @BeforeEach
    public void init() {
        keyValueEntityManagerFactory = KeyValueEntityManagerFactoryUtils.get();
        bucketManager = (HazelcastBucketManager) keyValueEntityManagerFactory.getBucketManager("movies-entity");

        bucketManager.put("matrix", new Movie("Matrix", 1999, false));
        bucketManager.put("star_wars", new Movie("Star Wars: The Last Jedi", 2017, true));
        bucketManager.put("grease", new Movie("Grease", 1978, false));
        bucketManager.put("justice_league", new Movie("Justice league", 2017, true));
        bucketManager.put("avengers", new Movie("The Avengers", 2012, false));
    }

    @Test
    public void shouldReturnWhenStringQueryIsNull() {
        assertThrows(NullPointerException.class, () -> bucketManager.query((String) null));
    }

    @Test
    public void shouldReturnWhenPredicateQueryIsNull() {
        assertThrows(NullPointerException.class, () -> bucketManager.sql((Predicate<? extends Object, ? extends Object>) null));
    }

    @Test
    public void shouldReturnActive() {
        Collection<Value> result = bucketManager.sql("active");
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnActiveAndGreaterThan2000() {
        Collection<Value> result = bucketManager.sql("NOT active AND year > 1990");
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnEqualsMatrix() {
        Collection<Value> result = bucketManager.sql("name = Matrix");
        assertEquals(1, result.size());
    }


    @Test
    public void shouldReturnActivePredicate() {
        Collection<Value> result = bucketManager.sql(equal("active", true));
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnActiveAndGreaterThan2000Predicate() {
        Predicate predicate = and(equal("active", false), greaterEqual("year", 1990));
        Collection<Value> result = bucketManager.sql(predicate);
        assertEquals(2, result.size());
    }

    @Test
    public void shouldReturnEqualsMatrixPredicate() {
        Predicate predicate = equal("name", "Matrix");
        Collection<Value> result = bucketManager.sql(predicate);
        assertEquals(1, result.size());
    }

    @Test
    public void shouldReturnEqualsNameParam() {
        Collection<Value> result = bucketManager.sql("name = :name", singletonMap("name", "Matrix"));
        assertEquals(1, result.size());
    }

    @Test
    public void shouldReturnNameAndYearParam() {
        Map<String, Object> params = new HashMap<>();
        params.put("name", "Matrix");
        params.put("year", 1900);
        Collection<Value> result = bucketManager.sql("name = :name AND year > :year", params);

        assertEquals(1, result.size());
    }

    @Test
    public void shouldReturnLikeNameParam() {
        Collection<Value> result = bucketManager.sql("name like :name", singletonMap("name", "Mat%"));
        assertEquals(1, result.size());
    }

}
