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
package org.jnosql.diana.cassandra.column;

import org.hamcrest.Matchers;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.column.Column;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;


public class DefaultUDTTest {


    @Test
    public void shouldCreateUDT() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder().withName("name")
                .withTypeName("fullname")
                .addAll(columns).build();

        assertEquals("fullname", udt.getUserType());
        assertEquals("name", udt.getName());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldReturnErrorWhenNameIsNull() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder()
                .withTypeName("fullname")
                .addAll(columns).build();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldReturnErrorWhenTypeNameIsNull() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder()
                .withName("name")
                .addAll(columns).build();
    }

    @Test
    public void shouldReturnGetType() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder().withName("name")
                .withTypeName("fullname")
                .addAll(columns).build();

        List<Column> udtColumn = udt.get(new TypeReference<List<Column>>() {
        });

        assertThat(columns, Matchers.containsInAnyOrder(Column.of("firstname", "Ada"), Column.of("lastname", "Lovelace")));

    }

}