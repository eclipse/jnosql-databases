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
package org.jnosql.diana.couchbase.key;

import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.jnosql.diana.api.key.KeyValueEntity;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;


public class CouchbaseBucketManagerTest {


    private static final String KEY_SORO = "soro";
    private static final String KEY_OTAVIO = "otavio";
    private BucketManager keyValueEntityManager;

    private BucketManagerFactory keyValueEntityManagerFactory;

    private User userOtavio = new User(KEY_OTAVIO);
    private KeyValueEntity entityOtavio = KeyValueEntity.of(KEY_OTAVIO, Value.of(userOtavio));

    private User userSoro = new User(KEY_SORO);
    private KeyValueEntity soroEntity = KeyValueEntity.of(KEY_SORO, Value.of(userSoro));

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager("default");
    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager("default");
        keyValueEntityManager.remove(KEY_OTAVIO);
        keyValueEntityManager.remove(KEY_SORO);
    }


    @Test
    public void shouldPutValue() {
        keyValueEntityManager.put(KEY_OTAVIO, userOtavio);
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));
    }

    @Test
    public void shouldPutKeyValue() {
        keyValueEntityManager.put(entityOtavio);
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));
    }

    @Test
    public void shouldPutIterableKeyValue() {


        keyValueEntityManager.put(asList(soroEntity, entityOtavio));
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));

        Optional<Value> soro = keyValueEntityManager.get(KEY_SORO);
        assertTrue(soro.isPresent());
        assertEquals(userSoro, soro.get().get(User.class));
    }

    @Test
    public void shouldMultiGet() {
        User user = new User(KEY_OTAVIO);
        KeyValueEntity keyValue = KeyValueEntity.of(KEY_OTAVIO, Value.of(user));
        keyValueEntityManager.put(keyValue);
        assertNotNull(keyValueEntityManager.get(KEY_OTAVIO));


    }

    @Test
    public void shouldRemoveKey() {

        keyValueEntityManager.put(entityOtavio);
        assertTrue(keyValueEntityManager.get(KEY_OTAVIO).isPresent());
        keyValueEntityManager.remove(KEY_OTAVIO);
        assertFalse(keyValueEntityManager.get(KEY_OTAVIO).isPresent());
    }

    @Test
    public void shouldRemoveMultiKey() {

        keyValueEntityManager.put(asList(soroEntity, entityOtavio));
        List<String> keys = asList(KEY_OTAVIO, KEY_SORO);
        Iterable<Value> values = keyValueEntityManager.get(keys);
        assertThat(StreamSupport.stream(values.spliterator(), false).map(value -> value.get(User.class)).collect(Collectors.toList()), containsInAnyOrder(userOtavio, userSoro));
        keyValueEntityManager.remove(keys);
        Iterable<Value> users = values;
        assertEquals(0L, StreamSupport.stream(keyValueEntityManager.get(keys).spliterator(), false).count());
    }


}