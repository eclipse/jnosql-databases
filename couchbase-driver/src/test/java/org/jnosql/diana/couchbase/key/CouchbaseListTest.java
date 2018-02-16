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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class CouchbaseListTest {

    private ProductCart banana = new ProductCart("banana", BigDecimal.ONE);
    private ProductCart orange = new ProductCart("orange", BigDecimal.ONE);
    private ProductCart waterMelon = new ProductCart("waterMelon", BigDecimal.TEN);
    private ProductCart melon = new ProductCart("melon", BigDecimal.ONE);

    private CouchbaseBucketManagerFactory keyValueEntityManagerFactory;

    private List<ProductCart> fruits;

    @BeforeEach
    public void init() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        keyValueEntityManagerFactory = configuration.get();
        fruits = keyValueEntityManagerFactory.getList(CouchbaseUtil.BUCKET_NAME, ProductCart.class);
    }

    @AfterAll
    public static void afterClass() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        BucketManagerFactory keyValueEntityManagerFactory = configuration.get();
        BucketManager keyValueEntityManager = keyValueEntityManagerFactory.getBucketManager(CouchbaseUtil.BUCKET_NAME);
        keyValueEntityManager.remove("jnosql:list");
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
    public void shouldAddAllIterable() {
        fruits.addAll(asList(banana, waterMelon, orange, melon));
        assertThat(fruits, contains(banana, waterMelon, orange, melon));
    }

    @Test
    public void shouldAddAllIterableWithIndex() {
        fruits.addAll(asList(orange, melon));
        fruits.addAll(1, asList(banana, waterMelon));
        assertThat(fruits, contains(orange, banana, waterMelon, melon));
    }

    @Test
    public void shouldAddWithIndex() {
        fruits.addAll(asList(banana, waterMelon));
        fruits.add(0, orange);
        assertThat(fruits, contains(orange, banana, waterMelon));
    }

    @Test
    public void shouldRemove() {
        fruits.addAll(asList(banana, waterMelon));
        fruits.remove(banana);
        assertTrue(fruits.size() == 1);
        assertEquals(fruits.get(0), waterMelon);
    }

    @Test
    public void shouldRemoveWithIndex() {
        fruits.addAll(asList(banana, waterMelon));
        fruits.remove(1);
        assertTrue(fruits.size() == 1);
        assertEquals(fruits.get(0), banana);
    }

    @Test
    public void shouldRemoveAll() {
        fruits.addAll(asList(banana, waterMelon, orange, melon));
        fruits.removeAll(asList(banana, melon));
        assertTrue(fruits.size() == 2);
        assertThat(fruits, containsInAnyOrder(waterMelon, orange));
    }

    @Test
    public void shouldContainsValue() {
        fruits.addAll(asList(banana, waterMelon));
        assertTrue(fruits.contains(waterMelon));
    }

    @Test
    public void shouldContainsAllValues() {
        fruits.addAll(asList(banana, waterMelon));
        assertTrue(fruits.containsAll(asList(waterMelon, banana)));
    }

    @AfterEach
    public void end() {
        fruits.clear();
    }
}