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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;


public class CouchbaseListTest {

    private static final String FRUITS = "default";
    private ProductCart banana = new ProductCart("banana", BigDecimal.ONE);
    private ProductCart orange = new ProductCart("orange", BigDecimal.ONE);
    private ProductCart waterMelon = new ProductCart("waterMelon", BigDecimal.TEN);
    private ProductCart melon = new ProductCart("melon", BigDecimal.ONE);

    private CouchbaseBucketManagerFactory keyValueEntityManagerFactory;

    private List<ProductCart> fruits;

    @Before
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        fruits = keyValueEntityManagerFactory.getList(FRUITS, ProductCart.class);
    }

    @AfterClass
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager("default");
        keyValueEntityManager.remove("default:list");
    }

    @Test
    public void shouldReturnsList() {
        assertNotNull(fruits);
    }

    @Test
    public void shouldAddList() {
        assertTrue(fruits.isEmpty());
        fruits.add(banana);
        assertFalse(fruits.isEmpty());
        ProductCart banana = fruits.get(0);
        assertNotNull(banana);
        assertEquals(banana.getName(), "banana");
    }

    @Test
    public void shouldRemoveList() {
        fruits.add(banana);
    }


    @After
    public void end() {
        fruits.clear();
    }
}