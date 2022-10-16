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

import jakarta.nosql.Value;
import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import jakarta.nosql.keyvalue.KeyValueEntity;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.DatabaseContainer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseBucketManagerTest {

    private static final String KEY_SORO = "soro";
    private static final String KEY_OTAVIO = "otavio";
    private BucketManager keyValueEntityManager;

    private BucketManagerFactory keyValueEntityManagerFactory;

    private User userOtavio = new User(KEY_OTAVIO);
    private KeyValueEntity entityOtavio = KeyValueEntity.of(KEY_OTAVIO, Value.of(userOtavio));

    private User userSoro = new User(KEY_SORO);
    private KeyValueEntity soroEntity = KeyValueEntity.of(KEY_SORO, Value.of(userSoro));

    @BeforeEach
    public void init() {
        CouchbaseKeyValueConfiguration configuration = DatabaseContainer.INSTANCE.getKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = DatabaseContainer.INSTANCE.getKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.delete(KEY_OTAVIO);
        keyValueEntityManager.delete(KEY_SORO);
    }

    @Test
    public void shouldPutValue() {
        keyValueEntityManager.put(KEY_OTAVIO, userOtavio);
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));
    }

    @Test
    public void shouldPutValues() {

        List<KeyValueEntity> entities = asList(KeyValueEntity.of(KEY_OTAVIO, userOtavio),
                KeyValueEntity.of(KEY_SORO, userSoro));

        keyValueEntityManager.put(entities);
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        assertEquals(userOtavio, otavio.get().get(User.class));

        Optional<Value> soro = keyValueEntityManager.get(KEY_SORO);
        assertTrue(soro.isPresent());
        assertEquals(userSoro, soro.get().get(User.class));
    }


    @Test
    public void shouldPutPrimitivesValues() {
        keyValueEntityManager.put("integer", 1);
        Optional<Value> integer = keyValueEntityManager.get("integer");
        assertTrue(integer.isPresent());
        assertEquals(Integer.valueOf(1), integer.get().get(Integer.class));
    }

    @Test
    public void shouldPutValueTtl() throws InterruptedException {

        keyValueEntityManager.put(KeyValueEntity.of(KEY_OTAVIO, userOtavio), Duration.ofSeconds(1L));

        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        Thread.sleep(2_000);
        otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertFalse(otavio.isPresent());
    }

    @Test
    public void shouldPutValuesTtl() throws InterruptedException {

        keyValueEntityManager.put(singleton(KeyValueEntity.of(KEY_OTAVIO, userOtavio)), Duration.ofSeconds(1L));
        Optional<Value> otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertTrue(otavio.isPresent());
        Thread.sleep(2_000);
        otavio = keyValueEntityManager.get(KEY_OTAVIO);
        assertFalse(otavio.isPresent());
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
        keyValueEntityManager.delete(KEY_OTAVIO);
        assertFalse(keyValueEntityManager.get(KEY_OTAVIO).isPresent());
    }

    @Test
    public void shouldRemoveMultiKey() {

        keyValueEntityManager.put(asList(soroEntity, entityOtavio));
        List<String> keys = asList(KEY_OTAVIO, KEY_SORO);
        Iterable<Value> values = keyValueEntityManager.get(keys);
        assertThat(StreamSupport.stream(values.spliterator(), false).map(value -> value.get(User.class)).collect(Collectors.toList()), Matchers.containsInAnyOrder(userOtavio, userSoro));
        keyValueEntityManager.delete(keys);
        Iterable<Value> users = values;
        assertEquals(0L, StreamSupport.stream(keyValueEntityManager.get(keys).spliterator(), false).count());
    }


}