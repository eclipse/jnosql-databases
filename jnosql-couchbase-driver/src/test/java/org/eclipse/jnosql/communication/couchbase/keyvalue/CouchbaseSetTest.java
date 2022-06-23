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
package org.eclipse.jnosql.communication.couchbase.keyvalue;

import jakarta.nosql.keyvalue.BucketManager;
import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.couchbase.CouchbaseUtil;
import org.eclipse.jnosql.communication.couchbase.configuration.CouchbaseKeyValueTcConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseSetTest {

    private BucketManagerFactory keyValueEntityManagerFactory;
    private User otavio = new User("otaviojava");
    private User felipe = new User("ffrancesquini");
    private Set<User> users;

    @BeforeEach
    public void init() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        users = keyValueEntityManagerFactory.getSet(CouchbaseUtil.BUCKET_NAME, User.class);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.delete("jnosql:set");
    }

    @Test
    public void shouldAddUsers() {
        assertTrue(users.isEmpty());
        users.add(otavio);
        assertTrue(users.size() == 1);

        users.remove(otavio);
        assertTrue(users.isEmpty());
    }


    @SuppressWarnings("unused")
    @Test
    public void shouldIterate() {

        users.add(otavio);
        users.add(otavio);
        users.add(felipe);
        users.add(otavio);
        users.add(felipe);
        int count = 0;
        for (User user : users) {
            count++;
        }
        assertTrue(count == 2);
        users.remove(otavio);
        users.remove(felipe);
        count = 0;
        for (User user : users) {
            count++;
        }
        assertTrue(count == 0);
    }

    @Test
    public void shouldContains() {
        users.add(otavio);
        assertTrue(users.contains(otavio));
        assertFalse(users.contains(felipe));
    }

    @Test
    public void shouldContainsAll() {
        users.add(otavio);
        assertTrue(users.containsAll(singleton(otavio)));
        assertFalse(users.contains(singleton(felipe)));
    }

    @Test
    public void shouldRemove() {
        users.add(otavio);
        assertTrue(users.remove(otavio));
        assertFalse(users.remove(felipe));
    }

    @Test
    public void shouldRemoveAll() {
        users.add(otavio);
        assertTrue(users.removeAll(singleton(otavio)));
    }


    @Test
    public void shouldAddAll() {
        assertTrue(users.isEmpty());
        users.addAll(Arrays.asList(otavio, felipe));
        assertEquals(2, users.size());
    }

    @Test
    public void shouldRetains(){
        users.addAll(asList(otavio));
        List<User> newUsers = new ArrayList<>();
        newUsers.add(otavio);
        newUsers.add(new User("gama"));
        users.retainAll(newUsers);
        assertEquals(1, newUsers.size());
    }

    @Test
    public void shouldToArray() {
        users.addAll(asList(otavio, felipe));
        Object[] objects = users.toArray();
        assertEquals(2, objects.length);
        assertTrue(Stream.of(objects).allMatch(User.class::isInstance));
    }

    @Test
    public void shouldToArrayParams() {
        users.addAll(asList(otavio, felipe));
        User[] objects = users.toArray(new User[2]);
        assertEquals(2, objects.length);
        assertTrue(Stream.of(objects).allMatch(User.class::isInstance));
    }

    @AfterEach
    public void dispose() {
        users.clear();
    }
}