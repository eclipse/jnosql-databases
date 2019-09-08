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

package org.eclipse.jnosql.diana.redis.keyvalue;


import jakarta.nosql.keyvalue.BucketManagerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.Queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RedisQueueTest {


    private BucketManagerFactory keyValueEntityManagerFactory;

    private Queue<LineBank> lineBank;

    @BeforeEach
    public void init() {
        keyValueEntityManagerFactory = RedisBucketManagerFactorySupplier.INSTANCE.get();
        lineBank = keyValueEntityManagerFactory.getQueue("physical-bank", LineBank.class);
    }

    @Test
    public void shouldPushInTheLine() {
        assertTrue(lineBank.add(new LineBank("Otavio", 25)));
        assertTrue(lineBank.size() == 1);
        LineBank otavio = lineBank.poll();
        assertEquals(otavio.getName(), "Otavio");
        assertNull(lineBank.poll());
        assertTrue(lineBank.isEmpty());
    }

    @Test
    public void shouldPeekInTheLine() {
        lineBank.add(new LineBank("Otavio", 25));
        LineBank otavio = lineBank.peek();
        assertNotNull(otavio);
        assertNotNull(lineBank.peek());
        LineBank otavio2 = lineBank.remove();
        assertEquals(otavio.getName(), otavio2.getName());
        boolean happendException = false;
        try {
            lineBank.remove();
        } catch (NoSuchElementException e) {
            happendException = true;
        }
        assertTrue(happendException);
    }

    @Test
    public void shouldElementInTheLine() {
        lineBank.add(new LineBank("Otavio", 25));
        assertNotNull(lineBank.element());
        assertNotNull(lineBank.element());
        lineBank.remove(new LineBank("Otavio", 25));
        boolean happendException = false;
        try {
            lineBank.element();
        } catch (NoSuchElementException e) {
            happendException = true;
        }
        assertTrue(happendException);
    }

    @SuppressWarnings("unused")
    @Test
    public void shouldIterate() {
        lineBank.add(new LineBank("Otavio", 25));
        lineBank.add(new LineBank("Gama", 26));
        int count = 0;
        for (LineBank line : lineBank) {
            count++;
        }
        assertTrue(count == 2);
        lineBank.remove();
        lineBank.remove();
        count = 0;
        for (LineBank line : lineBank) {
            count++;
        }
        assertTrue(count == 0);
    }

    @Test
    public void shouldClear() {
        lineBank.add(new LineBank("Otavio", 25));
        lineBank.clear();
        assertTrue(lineBank.isEmpty());
    }

    @AfterEach
    public void dispose() {
        lineBank.clear();
    }
}
