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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.NoSuchElementException;
import java.util.Queue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;


public class CouchbaseQueueTest {
    private BucketManagerFactory keyValueEntityManagerFactory;

    private Queue<User> users;

    @Before
    public void init() {

        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        users = keyValueEntityManagerFactory.getQueue("default", User.class);


    }

    @Test
    public void shouldPushInTheLine() {
        assertTrue(users.add(new User("Otavio")));
        assertTrue(users.size() == 1);
        User otavio = users.poll();
        assertEquals(otavio.getNickName(), "Otavio");
        assertNull(users.poll());
        assertTrue(users.isEmpty());
    }

    @Test
    public void shouldPeekInTheLine() {
        users.add(new User("Otavio"));
        User otavio = users.peek();
        assertNotNull(otavio);
        assertNotNull(users.peek());
        User otavio2 = users.remove();
        assertEquals(otavio.getNickName(), otavio2.getNickName());
        boolean happendException = false;
        try {
            users.remove();
        } catch (NoSuchElementException e) {
            happendException = true;
        }
        assertTrue(happendException);
    }

    @Test
    public void shouldElementInTheLine() {
        users.add(new User("Otavio"));
        assertNotNull(users.element());
        assertNotNull(users.element());
        users.remove(new User("Otavio"));
        boolean happendException = false;
        try {
            users.element();
        } catch (NoSuchElementException e) {
            happendException = true;
        }
        assertTrue(happendException);
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldIterate() {
        users.add(new User("Otavio"));
        users.add(new User("Gama"));
        int count = 0;
        for (User line : users) {
            count++;
        }
        assertTrue(count == 2);
        users.remove();
        users.remove();
        count = 0;
        for (User line : users) {
            count++;
        }
        assertTrue(count == 0);
    }

    @After
    public void dispose() {
        users.clear();
    }
}