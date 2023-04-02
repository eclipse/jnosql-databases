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
 */
package org.eclipse.jnosql.communication.arangodb.document;

import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Map;

import static org.eclipse.jnosql.communication.document.DocumentQuery.select;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.NAMED;
import static org.eclipse.jnosql.communication.driver.IntegrationTest.MATCHES;
import static org.junit.jupiter.api.Assertions.assertEquals;

@EnabledIfSystemProperty(named = NAMED, matches = MATCHES)
public class QueryAQLConverterTest {

    @Test
    public void shouldRunEqualsQuery() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value").build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name RETURN c", aql);

    }

    @Test
    public void shouldRunEqualsQueryAnd() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .and("age").lte(10)
                .build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name AND  c.age <= @age RETURN c", aql);

    }

    @Test
    public void shouldRunEqualsQueryOr() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .or("age").lte(10)
                .build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name OR  c.age <= @age RETURN c", aql);

    }

    @Test
    public void shouldRunEqualsQuerySort() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc().build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC RETURN c", aql);
    }

    @Test
    public void shouldRunEqualsQuerySort2() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .orderBy("name").asc()
                .orderBy("age").desc().build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name SORT  c.name ASC , c.age DESC RETURN c", aql);
    }


    @Test
    public void shouldRunEqualsQueryLimit() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .limit(5).build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 5 RETURN c", aql);

    }

    @Test
    public void shouldRunEqualsQueryLimit2() {
        DocumentQuery query = select().from("collection")
                .where("name").eq("value")
                .skip(1).limit(5).build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  c.name == @name LIMIT 1, 5 RETURN c", aql);
    }

    @Test
    public void shouldRunEqualsQueryNot() {
        DocumentQuery query = select().from("collection")
                .where("name").not().eq("value").build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals("value", values.get("name"));
        assertEquals("FOR c IN collection FILTER  NOT  c.name == @name RETURN c", aql);

    }


    @Test
    public void shouldNegate() {
        DocumentQuery query = select().from("collection")
                .where("city").not().eq("Assis")
                .and("name").eq("Otavio")
                .or("name").not().eq("Lucas").build();

        AQLQueryResult convert = QueryAQLConverter.select(query);
        String aql = convert.getQuery();
        Map<String, Object> values = convert.getValues();
        assertEquals(3, values.size());
        assertEquals("Assis", values.get("city"));
        assertEquals("Otavio", values.get("name"));
        assertEquals("Lucas", values.get("name_1"));
        assertEquals("FOR c IN collection FILTER  NOT  c.city == @city AND  c.name == @name OR  NOT  c.name == @name_1 RETURN c", aql);

    }

}