/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.communication.hazelcast.keyvalue;


import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.model.User;
import org.eclipse.jnosql.communication.hazelcast.keyvalue.util.KeyValueEntityManagerFactoryUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SetTest {


    private BucketManagerFactory keyValueEntityManagerFactory;
    private User userOtavioJava = new User("otaviojava");
    private User felipe = new User("ffrancesquini");
    private Set<User> users;

    @BeforeEach
    public void init() {
        keyValueEntityManagerFactory =  KeyValueEntityManagerFactoryUtils.get();
        users = keyValueEntityManagerFactory.getSet("social-media", User.class);
    }

    @Test
    public void shouldAddUsers() {
        assertTrue(users.isEmpty());
        users.add(userOtavioJava);
        assertEquals(1, users.size());

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
        for (User user: users) {
            count++;
        }
        assertEquals(2, count);
        users.remove(userOtavioJava);
        users.remove(felipe);
        count = 0;
        for (User user: users) {
            count++;
        }
        assertEquals(0, count);
    }

    @AfterEach
    public void dispose() {
        users.clear();
    }
}
