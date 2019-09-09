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
package org.eclipse.jnosql.diana.cassandra.column;

import com.datastax.driver.core.querybuilder.BuiltStatement;
import jakarta.nosql.column.ColumnQuery;
import org.junit.jupiter.api.Test;

import static jakarta.nosql.column.ColumnQuery.select;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    public void shouldReturnErrorWhenUseOr() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .or("age").lte(10)
                .build();

        assertThrows(UnsupportedOperationException.class, () -> {
            QueryUtils.select(query, "keyspace");
        });
    }

    @Test
    public void shouldRunEqualsQuerySort() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc().build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));

        assertEquals("SELECT * FROM keyspace.collection WHERE name='value' ORDER BY name ASC;", cql);
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
        assertEquals("SELECT * FROM keyspace.collection WHERE name='value' ORDER BY name ASC,age DESC;", cql);
    }


    @Test
    public void shouldRunEqualsQueryLimit() {
        ColumnQuery query = select().from("collection")
                .where("name").eq("value")
                .limit(5).build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("value", select.getObject(0));
        assertEquals("SELECT * FROM keyspace.collection WHERE name='value' LIMIT 5;", cql);

    }


    @Test
    public void shouldReturnErrorWhenUseNotOperator() {
        ColumnQuery query = select().from("collection")
                .where("name").not().eq("value").build();

        assertThrows(UnsupportedOperationException.class, () -> {
            QueryUtils.select(query, "keyspace");
        });

    }


    @Test
    public void shouldNegate() {
        ColumnQuery query = select().from("collection")
                .where("city").eq("Assis")
                .and("name").eq("Otavio")
                .and("name").eq("Lucas").build();

        BuiltStatement select = QueryUtils.select(query, "keyspace");
        String cql = select.toString();
        assertEquals("Assis", select.getObject(0));
        assertEquals("Otavio", select.getObject(1));
        assertEquals("Lucas", select.getObject(2));
        assertEquals("SELECT * FROM keyspace.collection WHERE city='Assis' AND name='Otavio' AND name='Lucas';", cql);

    }

}