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
                .addUDT(columns).build();

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
                .addUDT(columns).build();
    }

    @Test(expected = IllegalStateException.class)
    public void shouldReturnErrorWhenTypeNameIsNull() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder()
                .withName("name")
                .addUDT(columns).build();
    }

    @Test
    public void shouldReturnGetType() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder().withName("name")
                .withTypeName("fullname")
                .addUDT(columns).build();

        List<Column> udtColumn = udt.get(new TypeReference<List<Column>>() {
        });

        assertThat(columns, Matchers.containsInAnyOrder(Column.of("firstname", "Ada"), Column.of("lastname", "Lovelace")));

    }

}