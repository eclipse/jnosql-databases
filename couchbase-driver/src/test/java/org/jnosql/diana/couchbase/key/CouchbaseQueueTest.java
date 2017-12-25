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
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.junit.After;
import org.junit.AfterClass;
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
        users = keyValueEntityManagerFactory.getQueue(CouchbaseUtil.BUCKET_NAME, User.class);


    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.remove("jnosql:queue");
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
        assertNull(users.remove());
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