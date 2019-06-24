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
package org.jnosql.diana.driver;

import jakarta.nosql.TypeReference;
import jakarta.nosql.Value;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ValueJSONTest {

    @Test
    public void shouldReturnErrorWhenElementIsNull() {
        assertThrows(NullPointerException.class, () -> ValueJSON.of(null));
    }

    @Test
    public void shouldConvertType() {
        AtomicInteger number = new AtomicInteger(5_000);
        Value value = ValueJSON.of(number);
        assertEquals(Integer.valueOf(5_000), value.get(Integer.class));
        assertEquals("5000", value.get(String.class));
    }

    @Test
    public void shouldConvertMapIgnoringKeyValue() {
        Map<Integer, List<String>> map = Collections.singletonMap(10, Arrays.asList("1", "2", "3"));
        Value value = ValueJSON.of(map);
        Map<String, List<String>> result = value.get(new TypeReference<Map<String, List<String>>>(){});
        List<String> valueResult = result.get("10");
        assertThat(result.keySet(), containsInAnyOrder("10"));
        assertThat(valueResult, containsInAnyOrder("1", "2", "3"));
    }

}