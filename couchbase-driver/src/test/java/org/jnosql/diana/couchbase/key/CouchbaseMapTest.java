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

import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class CouchbaseMapTest {
    private BucketManagerFactory entityManagerFactory;

    private User mammals = new User("lion");
    private User fishes = new User("redfish");

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        entityManagerFactory = configuration.get();
        entityManagerFactory.getMap("default", String.class, User.class).clear();
    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager("default");
        keyValueEntityManager.remove("default:map");
    }

    @Test
    public void shouldPutAndGetMap() {
        Map<String, User> vertebrates = entityManagerFactory.getMap("default", String.class, User.class);
        assertTrue(vertebrates.isEmpty());

        assertNull(vertebrates.put("mammals", mammals));
        User species = vertebrates.get("mammals");
        assertNotNull(species);
        assertEquals(species.getNickName(), mammals.getNickName());
        assertTrue(vertebrates.size() == 1);
    }

    @Test
    public void shouldVerifyExist() {

        Map<String, User> vertebrates = entityManagerFactory.getMap("default", String.class, User.class);
        vertebrates.put("mammals", mammals);
        assertTrue(vertebrates.containsKey("mammals"));
        Assert.assertFalse(vertebrates.containsKey("redfish"));

        assertTrue(vertebrates.containsValue(mammals));
        Assert.assertFalse(vertebrates.containsValue(fishes));
    }


}