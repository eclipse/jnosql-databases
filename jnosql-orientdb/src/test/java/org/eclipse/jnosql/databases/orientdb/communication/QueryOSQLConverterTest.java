/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
 *   Lucas Furlaneto
 */
package org.eclipse.jnosql.databases.orientdb.communication;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.eclipse.jnosql.communication.semistructured.SelectQuery.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class QueryOSQLConverterTest {

    @Test
    public void shouldRunEqualsQuery() {
        var query = select().from("collection")
                .where("name").eq("value").build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        String sql = convert.query();
        List<Object> values = convert.params();
        assertEquals("value", values.get(0));
        assertEquals("SELECT FROM collection WHERE name = ?", sql);
    }

    @Test
    public void shouldRunEqualsQueryAnd() {
        var query = select().from("collection")
                .where("name").eq("value")
                .and("age").lte(10)
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        String sql = convert.query();
        List<Object> values = convert.params();
        assertEquals("value", values.get(0));
        assertEquals(10, values.get(1));
        assertEquals("SELECT FROM collection WHERE name = ? AND age <= ?", sql);
    }

    @Test
    public void shouldRunEqualsQueryOr() {
        var query = select().from("collection")
                .where("name").eq("value")
                .or("age").lte(10)
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        String sql = convert.query();
        List<Object> values = convert.params();
        assertEquals("value", values.get(0));
        assertEquals(10, values.get(1));
        assertEquals("SELECT FROM collection WHERE name = ? OR age <= ?", sql);
    }

    @Test
    public void shouldRunEqualsQueryNot() {
        var query = select().from("collection")
                .where("name").not().eq("value").build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        String sql = convert.query();
        List<Object> values = convert.params();
        assertEquals("value", values.get(0));
        assertEquals("SELECT FROM collection WHERE NOT (name = ?)", sql);
    }

    @Test
    public void shouldNegate() {
        var query = select().from("collection")
                .where("city").not().eq("Assis")
                .and("name").eq("Otavio")
                .or("name").not().eq("Lucas").build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        String sql = convert.query();
        List<Object> values = convert.params();
        assertEquals(3, values.size());
        assertEquals("Assis", values.get(0));
        assertEquals("Otavio", values.get(1));
        assertEquals("Lucas", values.get(2));
        assertEquals("SELECT FROM collection WHERE NOT (city = ?) AND name = ? OR NOT (name = ?)", sql);
    }

    @Test
    public void shouldPaginateWithStart() {
        var query = select().from("collection")
                .skip(10)
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection SKIP 10", convert.query());
    }

    @Test
    public void shouldPaginateWithLimit() {
        var query = select().from("collection")
                .limit(100)
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection LIMIT 100", convert.query());
    }

    @Test
    public void shouldPaginateWithStartAndLimit() {
        var query = select().from("collection")
                .skip(10)
                .limit(100)
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection SKIP 10 LIMIT 100", convert.query());
    }

    @Test
    public void shouldSortAsc() {
        var query = select().from("collection")
                .orderBy("name").asc()
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection ORDER BY name ASC", convert.query());
    }

    @Test
    public void shouldSortDesc() {
        var query = select().from("collection")
                .orderBy("name").desc()
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection ORDER BY name DESC", convert.query());
    }

    @Test
    public void shouldMultipleSort() {
        var query = select().from("collection")
                .orderBy("name").asc()
                .orderBy("age").desc()
                .build();

        QueryOSQLConverter.Query convert = QueryOSQLConverter.select(query);
        assertEquals("SELECT FROM collection ORDER BY name ASC, age DESC", convert.query());
    }
}
