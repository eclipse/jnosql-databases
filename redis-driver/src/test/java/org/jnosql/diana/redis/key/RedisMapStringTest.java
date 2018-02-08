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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.*;

public class RedisMapStringTest {

    private BucketManagerFactory entityManagerFactory;

    private static final String MAMMALS = "mammals";
    private static final String FISHES = "fishes";
    private static final String AMPHIBIANS = "amphibians";
    private static final String BUCKET_NAME = "vertebrates_string";

    private Map<String, String> vertebrates;

    @BeforeEach
    public void init() {
        entityManagerFactory = RedisTestUtils.get();
        vertebrates = entityManagerFactory.getMap(BUCKET_NAME, String.class, String.class);
    }

    @Test
    public void shouldPutAndGetMap() {
        assertTrue(vertebrates.isEmpty());

        assertNotNull(vertebrates.put(MAMMALS, MAMMALS));
        String species = vertebrates.get(MAMMALS);
        assertNotNull(species);
        assertEquals(MAMMALS, vertebrates.get(MAMMALS));
        assertTrue(vertebrates.size() == 1);
    }

    @Test
    public void shouldVerifyExist() {
        vertebrates.put(MAMMALS, MAMMALS);
        assertTrue(vertebrates.containsKey(MAMMALS));
        assertFalse(vertebrates.containsKey(FISHES));

        assertTrue(vertebrates.containsValue(MAMMALS));
        assertFalse(vertebrates.containsValue(FISHES));
    }

    @Test
    public void shouldShowKeyAndValues() {
        vertebrates.put(MAMMALS, MAMMALS);
        vertebrates.put(FISHES, FISHES);
        vertebrates.put(AMPHIBIANS, AMPHIBIANS);

        Set<String> keys = vertebrates.keySet();
        Collection<String> collectionSpecies = vertebrates.values();

        assertTrue(keys.size() == 3);
        assertTrue(collectionSpecies.size() == 3);
        assertNotNull(vertebrates.remove(MAMMALS));
        assertNull(vertebrates.remove(MAMMALS));
        assertNull(vertebrates.get(MAMMALS));
        assertTrue(vertebrates.size() == 2);
    }

    @Test
    public void shouldRemove() {
        vertebrates.put(MAMMALS, MAMMALS);
        vertebrates.put(FISHES, FISHES);
        vertebrates.put(AMPHIBIANS, AMPHIBIANS);

        vertebrates.remove(FISHES);
        assertTrue(vertebrates.size() == 2);
        assertThat(vertebrates, not(hasKey(FISHES)));
    }

    @Test
    public void shouldClear() {
        vertebrates.put(MAMMALS, MAMMALS);
        vertebrates.put(FISHES, FISHES);

        vertebrates.clear();
        assertTrue(vertebrates.isEmpty());
    }

    @AfterEach
    public void dispose() {
        vertebrates.clear();
    }

}
