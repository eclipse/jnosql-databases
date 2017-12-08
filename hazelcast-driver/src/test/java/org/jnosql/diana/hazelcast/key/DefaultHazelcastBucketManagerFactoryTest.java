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
package org.jnosql.diana.hazelcast.key;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class DefaultHazelcastBucketManagerFactoryTest {

    private HazelCastBucketManagerFactory managerFactory;

    @Before
    public void setUp() {
        HazelcastKeyValueConfiguration configuration = new HazelcastKeyValueConfiguration();
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
        Map<String, String> map = managerFactory.getMap("queue_sample", String.class, String.class);
        assertNotNull(map);
    }


    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterList() {

        managerFactory.getList(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterSet() {
        managerFactory.getList(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterQueue() {
        managerFactory.getList(null, String.class);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterMap() {
        managerFactory.getList(null, String.class);
    }

    //
    @Test
    public void shouldReturnListHazelcast() {
        List<String> list = managerFactory.getList("list_sample");
        assertNotNull(list);
    }

    @Test
    public void shouldReturnSetHazelcast() {
        Set<String> set = managerFactory.getSet("set_sample");
        assertNotNull(set);
    }

    @Test
    public void shouldReturnQueueHazelcast() {
        Queue<String> queue = managerFactory.getQueue("queue_sample");
        assertNotNull(queue);
    }

    @Test
    public void shouldReturnMapHazelcast() {
        Map<String, String> map = managerFactory.getMap("queue_sample");
        assertNotNull(map);
    }


    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterListHazelcast() {

        managerFactory.getList(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterSetHazelcast() {
        managerFactory.getList(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterQueueHazelcast() {
        managerFactory.getList(null);
    }

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenNullParameterMapHazelcast() {
        managerFactory.getList(null);
    }

}