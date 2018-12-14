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

import com.couchbase.client.java.datastructures.collections.CouchbaseArrayList;
import com.couchbase.client.java.datastructures.collections.CouchbaseArraySet;
import org.jnosql.diana.api.key.BucketManager;
import org.jnosql.diana.couchbase.configuration.CouchbaseKeyValueTcConfiguration;
import org.jnosql.diana.couchbase.CouchbaseUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DefaultCouchbaseBucketManagerFactoryTest {

    private CouchbaseBucketManagerFactory factory;

    @BeforeEach
    public void init() {
        CouchbaseKeyValueConfiguration configuration = CouchbaseKeyValueTcConfiguration.getTcConfiguration();
        factory = configuration.get();
    }

    @Test
    public void shouldReturnManager() {
        BucketManager database = factory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        assertNotNull(database);
    }

    @Test
    public void shouldReturnError() {
        assertThrows(NullPointerException.class, () -> factory.getBucketManager(null));
    }


    @Test
    public void shouldReturnList() {
        assertTrue(factory.getList("jnosql", String.class) instanceof CouchbaseArrayList);
        assertTrue(factory.getList("jnosql", User.class) instanceof CouchbaseList);
        assertTrue(factory.getList("jnosql", "jnosql", User.class) instanceof CouchbaseList);
        assertTrue(factory.getList("jnosql", "jnosql", String.class) instanceof CouchbaseArrayList);
    }

    @Test
    public void shouldReturnSet() {
        assertTrue(factory.getSet("jnosql", String.class) instanceof CouchbaseArraySet);
        assertTrue(factory.getSet("jnosql", User.class) instanceof CouchbaseSet);
        assertTrue(factory.getSet("jnosql", "jnosql", User.class) instanceof CouchbaseSet);
        assertTrue(factory.getSet("jnosql", "jnosql", String.class) instanceof CouchbaseArraySet);
    }

    @Test
    public void shouldReturnQueue() {
        assertTrue(factory.getQueue("jnosql", String.class) instanceof
                com.couchbase.client.java.datastructures.collections.CouchbaseQueue);
        assertTrue(factory.getQueue("jnosql", User.class) instanceof CouchbaseQueue);
        assertTrue(factory.getQueue("jnosql", "jnosql", User.class) instanceof CouchbaseQueue);
        assertTrue(factory.getQueue("jnosql", "jnosql", String.class) instanceof
                com.couchbase.client.java.datastructures.collections.CouchbaseQueue);
    }

    @Test
    public void shouldReturnMap() {
        assertTrue(factory.getMap("jnosql", String.class, String.class) instanceof
                com.couchbase.client.java.datastructures.collections.CouchbaseMap);
        assertTrue(factory.getMap("jnosql", String.class, User.class) instanceof CouchbaseMap);
        assertTrue(factory.getMap("jnosql", "jnosql", String.class, String.class) instanceof
                com.couchbase.client.java.datastructures.collections.CouchbaseMap);
        assertTrue(factory.getMap("jnosql", "jnosql", String.class, User.class) instanceof CouchbaseMap);
    }
}