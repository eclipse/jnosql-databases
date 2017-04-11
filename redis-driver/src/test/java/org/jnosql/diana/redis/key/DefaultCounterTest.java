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

import org.junit.Before;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


public class DefaultCounterTest {

    private static final String BRAZIL = "Brazil";
    private static final String USA = "USA";
    private static final String ENGLAND = "England";

    private RedisKeyValueEntityManagerFactory keyValueEntityManagerFactory;
    private Counter counter;

    @Before
    public void init() {
        keyValueEntityManagerFactory = RedisTestUtils.get();
        counter = keyValueEntityManagerFactory.getCounter("counter-redis");
        counter.delete();
    }

    @Test
    public void shouldIncrement() {
        assertEquals(1D, counter.increment());
        assertEquals(10D, counter.increment(9));
    }

    @Test
    public void shouldDecrement() {
        counter.increment(10.15);
        assertEquals(9.15D, counter.decrement());
        assertEquals(0.15D, counter.decrement(9));
    }

    @Test
    public void shouldGet() {
        counter.increment(10.15);
        assertEquals(10.15D, counter.get().doubleValue(), 0);
    }

    @Test
    public void shouldShouldExpires() throws InterruptedException {
        counter.increment(10.15);
        counter.expire(Duration.ofSeconds(1));
        Thread.sleep(2_000L);
        assertEquals(0D, counter.get().doubleValue(), 0);
    }

    @Test
    public void shouldPersist() throws InterruptedException {
        counter.increment(10.15);
        counter.expire(Duration.ofSeconds(1));
        counter.persist();
        Thread.sleep(2_000L);
        assertEquals(10.15D, counter.get().doubleValue(), 0);
    }

    @Test
    public void shouldDelete() {
        counter.increment(10.15);
        counter.delete();
        assertEquals(0D, counter.get().doubleValue(), 0);
    }
}