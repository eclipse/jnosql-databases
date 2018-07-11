/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.document.DocumentQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;

import javax.json.JsonObject;

import static org.jnosql.diana.api.document.query.DocumentQueryBuilder.select;

class MangoQueryConverterTest {

    private MangoQueryConverter converter = new MangoQueryConverter();

    @ParameterizedTest
    @JsonSource("select_all.json")
    public void shouldReturnSelectFromAll(JsonObject expected) {
        DocumentQuery query = select().from("person").build();
        JsonObject jsonObject = converter.apply(query);
        Assertions.assertEquals(expected, jsonObject);
    }

    @ParameterizedTest
    @JsonSource("select_fields.json")
    public void shouldReturnSelectFieldsFromAll(JsonObject expected) {
        DocumentQuery query = select("_id", "_rev").from("person").build();
        JsonObject jsonObject = converter.apply(query);
        Assertions.assertEquals(expected, jsonObject);
    }

    @ParameterizedTest
    @JsonSource("select_fields_skip_start.json")
    public void shouldReturnSelectFieldsLimitSkip(JsonObject expected) {
        DocumentQuery query = select("_id", "_rev").from("person").limit(10).skip(2).build();
        JsonObject jsonObject = converter.apply(query);
        Assertions.assertEquals(expected, jsonObject);
    }

    @ParameterizedTest
    @JsonSource("select_from_order.json")
    public void shouldReturnSelectFromOrder(JsonObject expected) {
        DocumentQuery query = select().from("person").orderBy("year").asc()
                .orderBy("name").desc().build();
        JsonObject jsonObject = converter.apply(query);
        Assertions.assertEquals(expected, jsonObject);
    }

    @ParameterizedTest
    @JsonSource("select_from_gt_order.json")
    public void shouldSelectFromGtAge(JsonObject expected) {
        DocumentQuery query = select().from("person").where("age").gt(10).build();
        JsonObject jsonObject = converter.apply(query);
        Assertions.assertEquals(expected, jsonObject);
    }
}