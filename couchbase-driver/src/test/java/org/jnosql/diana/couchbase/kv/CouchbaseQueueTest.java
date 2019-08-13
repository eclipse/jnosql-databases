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
package org.jnosql.diana.couchbase.kv;

import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.jnosql.diana.couchbase.configuration.CouchbaseKeyValueTcConfiguration;
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseQueueTest {
    private BucketManagerFactory keyValueEntityManagerFactory;

    private Queue<User> users;

    @BeforeEach
    public void init() {

        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        users = keyValueEntityManagerFactory.getQueue(CouchbaseUtil.BUCKET_NAME, User.class);


    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
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
        assertNull(users.remove());
    }

    @Test
    public void shouldShouldAddAll() {
        users.addAll(singleton(new User("Otavio")));
        assertEquals(1, users.size());
    }

    @Test
    public void shouldRemoveAll() {
        users.addAll(singleton(new User("Otavio")));
        assertTrue(users.removeAll(singleton(new User("Otavio"))));
        assertTrue(users.isEmpty());
    }

    @Test
    public void shouldContains() {
        users.add(new User("Otavio"));
        assertTrue(users.contains(new User("Otavio")));
        assertFalse(users.contains(new User("Poliana")));
    }

    @Test
    public void shouldContainsAll() {
        users.add(new User("Otavio"));
        assertTrue(users.containsAll(singleton(new User("Otavio"))));
        assertFalse(users.containsAll(singleton(new User("Poliana"))));
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

    @Test
    public void shouldRetains(){
        users.addAll(asList(new User("Otavio")));
        List<User> newUsers = new ArrayList<>();
        newUsers.add(new User("Otavio"));
        newUsers.add(new User("gama"));
        users.retainAll(newUsers);
        assertEquals(1, newUsers.size());
    }

    @Test
    public void shouldToArray() {
        users.addAll(asList(new User("Otavio"), new User("felipe")));
        Object[] objects = users.toArray();
        assertEquals(2, objects.length);
        assertTrue(Stream.of(objects).allMatch(User.class::isInstance));
    }

    @Test
    public void shouldToArrayParams() {
        users.addAll(asList(new User("Otavio"), new User("felipe")));
        User[] objects = users.toArray(new User[2]);
        assertEquals(2, objects.length);
        assertTrue(Stream.of(objects).allMatch(User.class::isInstance));
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

    @AfterEach
    public void dispose() {
        users.clear();
    }
}