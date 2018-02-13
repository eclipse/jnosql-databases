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
package org.jnosql.diana.orientdb.document;

import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.orientdb.document.QueryOSQLConverter.Query;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Disabled
class QueryOSQLConverterTest {

    @Test
    public void shouldRunEqualsQuery() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value").build();

        Query convert = QueryOSQLConverter.select(query);
        String osql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name RETURN c", osql);

    }

    @Test
    public void shouldRunEqualsQueryAnd() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .and("age").lte(10)
                .build();

        Query convert = QueryOSQLConverter.select(query);
        String osql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name AND  c.age <= @age RETURN c", osql);

    }

    @Test
    public void shouldRunEqualsQueryOr() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .or("age").lte(10)
                .build();

        Query convert = QueryOSQLConverter.select(query);
        String osql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name OR  c.age <= @age RETURN c", osql);

    }

    @Test
    public void shouldRunEqualsQuerySort() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc().build();

        Query convert = QueryOSQLConverter.select(query);
        String aql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC RETURN c", aql);
    }

    @Test
    public void shouldRunEqualsQuerySort2() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc()
                .orderBy("age").desc().build();

        Query convert = QueryOSQLConverter.select(query);
        String osql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC , c.age DESC RETURN c", osql);
    }


    @Test
    public void shouldRunEqualsQueryLimit() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .limit(5).build();

        Query convert = QueryOSQLConverter.select(query);
        String aql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 5 RETURN c", aql);

    }

    @Test
    public void shouldRunEqualsQueryLimit2() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .start(1).limit(5).build();

        Query convert = QueryOSQLConverter.select(query);
        String aql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 1, 5 RETURN c", aql);
    }

    @Test
    public void shouldRunEqualsQueryNot() {
        DocumentQuery query = select().from("collection")
                .where("name").not().eq("value").build();

        Query convert = QueryOSQLConverter.select(query);
        String osql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals("value", values.get(0));
        assertEquals("FOR c IN collection FILTER  NOT  c.name == @name RETURN c", osql);

    }


    @Test
    public void shouldNegate() {
        DocumentQuery query = select().from("collection")
                .where("city").not().eq("Assis")
                .and("name").eq("Otavio")
                .or("name").not().eq("Lucas").build();

        Query convert = QueryOSQLConverter.select(query);
        String aql = convert.getQuery();
        List<Object> values = convert.getParams();
        assertEquals(3, values.size());
        assertEquals("Assis", values.get(0));
        assertEquals("Otavio", values.get(1));
        assertEquals("Lucas", values.get(2));
        assertEquals("FOR c IN collection FILTER  NOT  c.city == @city AND  c.name == @name OR  NOT  c.name == @name_1 RETURN c", aql);

    }
}