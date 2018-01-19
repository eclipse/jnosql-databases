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

package org.jnosql.diana.redis.key;

import org.jnosql.diana.api.key.BucketManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

public class RedisMapStringTest {

    private BucketManagerFactory entityManagerFactory;


    @BeforeEach
    public void init() {
        entityManagerFactory = RedisTestUtils.get();
    }

    @Test
    public void shouldPutAndGetMap() {
        Map<String, String> vertebrates = entityManagerFactory.getMap("vertebrates_string", String.class, String.class);
        assertTrue(vertebrates.isEmpty());

        assertNotNull(vertebrates.put("mammals", "mammals"));
        String species = vertebrates.get("mammals");
        assertNotNull(species);
        assertEquals("mammals", vertebrates.get("mammals"));
        assertTrue(vertebrates.size() == 1);
    }

    @Test
    public void shouldVerifyExist() {

        Map<String, String> vertebrates = entityManagerFactory.getMap("vertebrates_string", String.class, String.class);
        vertebrates.put("mammals", "mammals");
        assertTrue(vertebrates.containsKey("mammals"));
        assertFalse(vertebrates.containsKey("redfish"));

        assertTrue(vertebrates.containsValue("mammals"));
        assertFalse(vertebrates.containsValue("fishes"));
    }

    @Test
    public void shouldShowKeyAndValues() {
        Map<String, String> vertebratesMap = new HashMap<>();
        vertebratesMap.put("mammals", "mammals");
        vertebratesMap.put("fishes", "fishes");
        vertebratesMap.put("amphibians", "amphibians");
        Map<String, String> vertebrates = entityManagerFactory.getMap("vertebrates_string", String.class, String.class);
        vertebrates.putAll(vertebratesMap);

        Set<String> keys = vertebrates.keySet();
        Collection<String> collectionSpecies = vertebrates.values();

        assertTrue(keys.size() == 3);
        assertTrue(collectionSpecies.size() == 3);
        assertNotNull(vertebrates.remove("mammals"));
        assertNull(vertebrates.remove("mammals"));
        assertNull(vertebrates.get("mammals"));
        assertTrue(vertebrates.size() == 2);
    }

    @AfterEach
    public void dispose() {
        Map<String, String> vertebrates = entityManagerFactory.getMap("vertebrates_string", String.class, String.class);
        vertebrates.clear();
    }

}
