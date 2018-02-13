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

import com.datastax.driver.core.querybuilder.BuiltStatement;
import org.jnosql.diana.api.column.ColumnQuery;
import org.jnosql.diana.api.column.query.ColumnQueryBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.jnosql.diana.api.column.query.ColumnQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.*;

class QueryUtilsTest {

    @Test
    public void shouldRunEqualsQuery() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value").build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("SELECT * FROM keyspace.collection WHERE name='value';", cql);

    }

    @Test
    public void shouldRunEqualsQueryAnd() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .and("age").lte(10)
                .build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("SELECT * FROM keyspace.collection WHERE name='value' AND age<=10;", cql);

    }

    @Test
    public void shouldRunEqualsQueryOr() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .or("age").lte(10)
                .build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("SELECT * FROM keyspace.collection WHERE name='value' OR age<=10;", cql);

    }

    @Test
    public void shouldRunEqualsQuerySort() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc().build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));

        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC RETURN c", cql);
    }

    @Test
    public void shouldRunEqualsQuerySort2() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc()
                .orderBy("age").desc().build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC , c.age DESC RETURN c", cql);
    }


    @Test
    public void shouldRunEqualsQueryLimit() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .limit(5).build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 5 RETURN c", cql);

    }

    @Test
    public void shouldRunEqualsQueryLimit2() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .start(1).limit(5).build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 1, 5 RETURN c", cql);
    }

    @Test
    public void shouldRunEqualsQueryNot() {
        ColumnQuery query = select().from("collection")
                .where("name").not().eq("value").build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("FOR c IN collection FILTER  NOT  c.name == @name RETURN c", cql);

    }


    @Test
    public void shouldNegate() {
        ColumnQuery query = select().from("collection")
                .where("city").not().eq("Assis")
                .and("name").eq("Otavio")
                .or("name").not().eq("Lucas").build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("Assis", select.getObject(0));
        assertEquals("Otavio", select.getObject(0));
        assertEquals("Lucas", select.getObject(0));
        assertEquals("FOR c IN collection FILTER  NOT  c.city == @city AND  c.name == @name OR  NOT  c.name == @name_1 RETURN c", cql);

    }

}