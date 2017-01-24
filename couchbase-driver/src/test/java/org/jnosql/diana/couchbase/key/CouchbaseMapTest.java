/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.couchbase.key;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class CouchbaseMapTest {
    private BucketManagerFactory entityManagerFactory;

    private User mammals = new User("lion");
    private User fishes = new User("redfish");
    private User amphibians = new User("crododile");

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        entityManagerFactory = configuration.get();
    }

    @Test
    public void shouldPutAndGetMap() {
        Map<String, User> vertebrates = entityManagerFactory.getMap("default", String.class, User.class);
        assertTrue(vertebrates.isEmpty());

        assertNotNull(vertebrates.put("mammals", mammals));
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