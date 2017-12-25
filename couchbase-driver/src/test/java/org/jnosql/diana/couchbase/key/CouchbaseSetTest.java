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

import java.util.Set;

import static org.junit.Assert.*;


public class CouchbaseSetTest {

    private BucketManagerFactory keyValueEntityManagerFactory;
    private User userOtavioJava = new User("otaviojava");
    private User felipe = new User("ffrancesquini");
    private Set<User> users;

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        users = keyValueEntityManagerFactory.getSet(CouchbaseUtil.BUCKET_NAME, User.class);
    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.remove("jnosql:set");
    }

    @Test
    public void shouldAddUsers() {
        assertTrue(users.isEmpty());
        users.add(userOtavioJava);
        assertTrue(users.size() == 1);

        users.remove(userOtavioJava);
        assertTrue(users.isEmpty());
    }


    @SuppressWarnings("unused")
    @Test
    public void shouldIterate() {

        users.add(userOtavioJava);
        users.add(userOtavioJava);
        users.add(felipe);
        users.add(userOtavioJava);
        users.add(felipe);
        int count = 0;
        for (User user : users) {
            count++;
        }
        assertTrue(count == 2);
        users.remove(userOtavioJava);
        users.remove(felipe);
        count = 0;
        for (User user : users) {
            count++;
        }
        assertTrue(count == 0);
    }

    @After
    public void dispose() {
        users.clear();
    }
}