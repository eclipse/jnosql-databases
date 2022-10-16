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

import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.DatabaseContainer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseMapTest {
    private BucketManagerFactory entityManagerFactory;

    private User mammals = new User("lion");
    private User fishes = new User("redfish");

    @BeforeEach
    public void init() {
        CouchbaseKeyValueConfiguration configuration = DatabaseContainer.INSTANCE.getKeyValueConfiguration();
        entityManagerFactory = configuration.get();
        entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class).clear();
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = DatabaseContainer.INSTANCE.getKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.delete("jnosql:map");
    }

    @Test
    public void shouldPutAndGetMap() {
        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        assertTrue(vertebrates.isEmpty());

        assertNull(vertebrates.put("mammals", mammals));
        User species = vertebrates.get("mammals");
        assertNotNull(species);
        assertEquals(species.getNickName(), mammals.getNickName());
        assertTrue(vertebrates.size() == 1);
    }

    @Test
    public void shouldVerifyExist() {

        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        vertebrates.put("mammals", mammals);
        assertTrue(vertebrates.containsKey("mammals"));
        assertFalse(vertebrates.containsKey("redfish"));

        assertTrue(vertebrates.containsValue(mammals));
        assertFalse(vertebrates.containsValue(fishes));
    }


    @Test
    public void shouldRemove() {
        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        vertebrates.put("mammals", mammals);
        assertNotNull(vertebrates.remove("mammals"));
        assertNull(vertebrates.remove("mammals"));
    }

    @Test
    public void shouldPutAll() {
        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        Map<String, User> map = Collections.singletonMap("mammals", mammals);
        vertebrates.putAll(map);

        assertFalse(vertebrates.isEmpty());
        assertTrue(vertebrates.containsKey("mammals"));
    }

    @Test
    public void shouldKeySet() {
        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        vertebrates.put("mammals", mammals);

        Set<String> keys = vertebrates.keySet();
        assertFalse(keys.isEmpty());
        assertTrue(keys.stream().anyMatch(String.class::isInstance));
    }

    @Test
    public void shouldEntrySet() {
        Map<String, User> vertebrates = entityManagerFactory.getMap(CouchbaseUtil.BUCKET_NAME, String.class, User.class);
        vertebrates.put("mammals", mammals);

        Set<Map.Entry<String, User>> entries = vertebrates.entrySet();
        assertFalse(entries.isEmpty());
        assertEquals(1, entries.size());
        Map.Entry<String, User> entry = entries.stream().findFirst().get();
        assertEquals("mammals", entry.getKey());
        assertEquals(mammals, entry.getValue());
    }

}