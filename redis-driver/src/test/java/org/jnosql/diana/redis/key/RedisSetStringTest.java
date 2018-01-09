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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;

public class RedisSetStringTest {


    private BucketManagerFactory keyValueEntityManagerFactory;

    private Set<String> users;

    @Before
    public void init() {
        keyValueEntityManagerFactory = RedisTestUtils.get();
        users = keyValueEntityManagerFactory.getSet("social-media", String.class);
    }

    @Test
    public void shouldAddUsers() {
        assertTrue(users.isEmpty());
        users.add("otaviojava");
        assertTrue(users.size() == 1);

        users.remove("otaviojava");
        assertTrue(users.isEmpty());
    }


    @SuppressWarnings("unused")
    @Test
    public void shouldIterate() {

        users.add("otaviojava");
        users.add("otaviojava");
        users.add("felipe");
        users.add("otaviojava");
        users.add("felipe");
        int count = 0;
        for (String user : users) {
            count++;
        }
        assertTrue(count == 2);
        users.remove("otaviojava");
        users.remove("felipe");
        count = 0;
        for (String user : users) {
            count++;
        }
        assertTrue(count == 0);
    }

    @After
    public void dispose() {
        users.clear();
    }
}
