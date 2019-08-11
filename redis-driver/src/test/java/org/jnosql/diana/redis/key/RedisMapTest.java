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

import jakarta.nosql.kv.BucketManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;

public class RedisMapTest {

    private BucketManagerFactory entityManagerFactory;

    private Species mammals = new Species("lion", "cow", "dog");
    private Species fishes = new Species("redfish", "glassfish");
    private Species amphibians = new Species("crododile", "frog");

    private Map<String, Species> vertebrates;

    @BeforeEach
    public void init() {
        entityManagerFactory = RedisBucketManagerFactorySupplier.INSTANCE.get();
        vertebrates = entityManagerFactory.getMap("vertebrates", String.class, Species.class);
    }

    @Test
    public void shouldPutAndGetMap() {
        assertNotNull(vertebrates.put("mammals", mammals));
        Species species = vertebrates.get("mammals");
        assertNotNull(species);
        assertEquals(species.getAnimals().get(0), mammals.getAnimals().get(0));
        assertTrue(vertebrates.size() == 1);
    }

    @Test
    public void shouldPutAll() {
        Map toPutAll = new HashMap<String, Species>();
        toPutAll.put("mammals", mammals);
        toPutAll.put("fishes", fishes);

        vertebrates.putAll(toPutAll);
        assertTrue(vertebrates.size() == 2);
    }

    @Test
    public void shouldVerifyExist() {
        vertebrates.put("mammals", mammals);
        assertTrue(vertebrates.containsKey("mammals"));
        assertFalse(vertebrates.containsKey("redfish"));

        assertTrue(vertebrates.containsValue(mammals));
        assertFalse(vertebrates.containsValue(fishes));
    }

    @Test
    public void shouldShowKeyAndValues() {
        vertebrates.put("mammals", mammals);
        vertebrates.put("fishes", fishes);
        vertebrates.put("amphibians", amphibians);

        Set<String> keys = vertebrates.keySet();
        Collection<Species> collectionSpecies = vertebrates.values();

        assertTrue(keys.size() == 3);
        assertTrue(collectionSpecies.size() == 3);
        assertNotNull(vertebrates.remove("mammals"));
        assertNull(vertebrates.remove("mammals"));
        assertNull(vertebrates.get("mammals"));
        assertTrue(vertebrates.size() == 2);
    }

    @Test
    public void shouldRemove() {
        vertebrates.put("mammals", mammals);
        vertebrates.put("fishes", fishes);
        vertebrates.put("amphibians", amphibians);

        vertebrates.remove("fishes");
        assertTrue(vertebrates.size() == 2);
        assertThat(vertebrates, not(hasKey(fishes)));
    }

    @Test
    public void shouldClear() {
        vertebrates.put("mammals", mammals);
        vertebrates.put("fishes", fishes);

        vertebrates.clear();
        assertTrue(vertebrates.isEmpty());
    }

    @AfterEach
    public void dispose() {
        vertebrates.clear();
    }

}
