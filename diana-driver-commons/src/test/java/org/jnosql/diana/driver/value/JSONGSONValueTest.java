/*
 * Copyright 2017 Eclipse Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.driver.value;

import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


public class JSONGSONValueTest {

    @Test(expected = NullPointerException.class)
    public void shouldReturnErrorWhenElementIsNull() {
        JSONGSONValue.of(null);
    }


    @Test
    public void shouldConvertType() {
        AtomicInteger number = new AtomicInteger(5_000);
        Value value = JSONGSONValue.of(number);
        assertEquals(Integer.valueOf(5_000), value.get(Integer.class));
        assertEquals("5000", value.get(String.class));
    }


    @Test
    public void shouldConvertMapIgnoringKeyValue() {
        Map<Integer, List<String>> map = Collections.singletonMap(10, Arrays.asList("1", "2", "3"));
        Value value = JSONGSONValue.of(map);
        Map<String, List<String>> result = value.get(new TypeReference<Map<String, List<String>>>(){});
        List<String> valueResult = result.get("10");
        assertThat(result.keySet(), containsInAnyOrder("10"));
        assertThat(valueResult, containsInAnyOrder("1", "2", "3"));
    }
}