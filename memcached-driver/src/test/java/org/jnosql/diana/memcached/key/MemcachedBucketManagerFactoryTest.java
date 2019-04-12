/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.jnosql.diana.memcached.key;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class MemcachedBucketManagerFactoryTest {

    private MemcachedBucketManagerFactory managerFactory;

    @BeforeEach
    public void setUp() {
        MemcachedKeyValueConfiguration configuration = new MemcachedKeyValueConfiguration();
        managerFactory = configuration.get();
    }


    @Test
    public void shouldReturnList() {
        List<String> list = managerFactory.getList("list_sample", String.class);
        assertNotNull(list);
    }

    @Test
    public void shouldReturnSet() {
        Set<String> set = managerFactory.getSet("set_sample", String.class);
        assertNotNull(set);
    }

    @Test
    public void shouldReturnQueue() {
        Queue<String> queue = managerFactory.getQueue("queue_sample", String.class);
        assertNotNull(queue);
    }

    @Test
    public void shouldReturnMap() {
        Map<String, String> map = managerFactory.getMap("map_sample", String.class, String.class);
        assertNotNull(map);
    }


    @Test
    public void shouldReturnErrorWhenNullParameterList() {
        assertThrows(NullPointerException.class, () -> managerFactory.getList(null, String.class));
    }

    @Test
    public void shouldReturnErrorWhenNullParameterSet() {
        assertThrows(NullPointerException.class, () -> managerFactory.getSet(null, String.class));
    }

    @Test
    public void shouldReturnErrorWhenNullParameterQueue() {
        assertThrows(NullPointerException.class, () -> managerFactory.getQueue(null, String.class));
    }

    @Test
    public void shouldReturnErrorWhenNullParameterMap() {
        assertThrows(NullPointerException.class, () -> managerFactory.getMap(null, String.class, String.class));
    }

}