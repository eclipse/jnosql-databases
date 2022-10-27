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
package org.eclipse.jnosql.communication.cassandra.column;

import jakarta.nosql.TypeReference;
import jakarta.nosql.column.Column;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class DefaultUDTTest {


    @Test
    public void shouldCreateUDT() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder("fullname").withName("name")
                .addUDT(columns).build();

        assertEquals("fullname", udt.getUserType());
        assertEquals("name", udt.getName());
    }

    @Test
    public void shouldReturnGetType() {
        List<Column> columns = new ArrayList<>();
        columns.add(Column.of("firstname", "Ada"));
        columns.add(Column.of("lastname", "Lovelace"));
        UDT udt = UDT.builder("fullname").withName("name")
                .addUDT(columns).build();

        List<Column> udtColumn = udt.get(new TypeReference<>() {
        });

        assertThat(columns).contains(Column.of("firstname", "Ada"),
                Column.of("lastname", "Lovelace"));
    }

}