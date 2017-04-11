/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package org.jnosql.diana.redis.key;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class DefaultSortedSetTest {

    private static final String BRAZIL = "Brazil";

    private RedisKeyValueEntityManagerFactory keyValueEntityManagerFactory;
    private SortedSet sortedSet;

    @Before
    public void init() {
        keyValueEntityManagerFactory = RedisTestUtils.get();
        sortedSet = keyValueEntityManagerFactory.getSortedSet("world-cup-2018");
        sortedSet.remove(BRAZIL);
    }

    @Test
    public void shouldAdd() {
        sortedSet.add(BRAZIL, 10);
    }

    @Test
    public void shouldIncrement() {
        sortedSet.add(BRAZIL, 10);
        Number points = sortedSet.increment(BRAZIL, 2);
        assertEquals(Long.valueOf(12), Long.valueOf(points.longValue()));
    }

    @Test
    public void shouldDecrement() {
        sortedSet.add(BRAZIL, 10);
        Number points = sortedSet.decrement(BRAZIL, 2);
        assertEquals(Long.valueOf(8), Long.valueOf(points.longValue()));
    }

    @Test
    public void shouldRemoveMember() {
        sortedSet.add(BRAZIL, 10);
        sortedSet.remove(BRAZIL);
        assertTrue(sortedSet.size() == 0);
    }

    @Test
    public void shouldShouldExpires() throws InterruptedException {
        sortedSet.add(BRAZIL, 10);
        sortedSet.expire(Duration.ofSeconds(1));
        Thread.sleep(2_000L);
        assertTrue(sortedSet.size() == 0);
    }

    @Test
    public void shouldPersist() throws InterruptedException {
        sortedSet.add(BRAZIL, 10);
        sortedSet.expire(Duration.ofSeconds(1));
        sortedSet.persist();
        Thread.sleep(2_000L);
        assertTrue(sortedSet.size() == 1);
    }

    @Test
    public void shouldRange() {
        sortedSet.add(BRAZIL, 1);
        sortedSet.add("USA", 2);
        sortedSet.add("England", 3);

        assertThat(sortedSet.range(2, 3), Matchers.contains(new DefaultRanking("USA", 2)));
    }

}