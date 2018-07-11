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

import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

final class MangoQueryConverter implements Function<DocumentQuery, JsonObject> {


    @Override
    public JsonObject apply(DocumentQuery documentQuery) {
        JsonObjectBuilder select = Json.createObjectBuilder();

        if (!documentQuery.getDocuments().isEmpty()) {
            select.add("fields", Json.createArrayBuilder(documentQuery.getDocuments()).build());
        }
        if (documentQuery.getLimit() > 0) {
            select.add("limit", documentQuery.getLimit());
        }

        if (documentQuery.getSkip() > 0) {
            select.add("skip", documentQuery.getSkip());
        }

        if (!documentQuery.getSorts().isEmpty()) {
            JsonArrayBuilder sorts = Json.createArrayBuilder();
            documentQuery.getSorts().stream().map(this::createSortObject).forEach(sorts::add);
            select.add("sort", sorts.build());
        }

        JsonObject selector = getSelector(documentQuery);
        return select.add("selector", selector).build();
    }

    private JsonObject createSortObject(Sort sort) {
        return Json.createObjectBuilder().add(sort.getName(), sort.getType().name().toLowerCase(Locale.US)).build();
    }

    private JsonObject getSelector(DocumentQuery documentQuery) {
        JsonObjectBuilder selector = Json.createObjectBuilder();
        selector.add(HttpExecute.ENTITY, documentQuery.getDocumentCollection());
        documentQuery.getCondition().ifPresent(d -> getSelector(d, selector));
        return selector.build();
    }

    private void getSelector(DocumentCondition condition, JsonObjectBuilder selector) {
        Document document = condition.getDocument();
        String name = document.getName();
        Object value = ValueUtil.convert(document.getValue());

        switch (condition.getCondition()) {
            case EQUALS:
                appendCondition(selector, name, value);
                return;
            case GREATER_THAN:
                appendCondition("$gt", name, value, selector);
                return;
            case GREATER_EQUALS_THAN:
                appendCondition("$gte", name, value, selector);
                return;
            case LESSER_THAN:
                appendCondition("$lt", name, value, selector);
                return;
            case LESSER_EQUALS_THAN:
                appendCondition("$lte", name, value, selector);
                return;
            case IN:
                appendCondition("$in", name, getArray(value), selector);
                return;
            case NOT:
                JsonObjectBuilder not = Json.createObjectBuilder();
                getSelector(DocumentCondition.class.cast(value), not);
                selector.add("$not", not.build());
                return;
            case AND:
                return;
            case OR:
                return;
            default:
                throw new UnsupportedOperationException("This operation is not supported at couchdb: " + condition.getCondition());

        }
    }

    private void appendCondition(String operator, String name, Object value, JsonObjectBuilder selector) {
        JsonObjectBuilder condition = Json.createObjectBuilder();
        appendCondition(condition, operator, value);
        selector.add(name, condition.build());
    }

    private void appendCondition(JsonObjectBuilder condition, String name, Object value) {
        if (value instanceof String) {
            condition.add(name, value.toString());
            return;
        }
        if (value instanceof Boolean) {
            condition.add(name, Boolean.class.cast(value));
            return;
        }
        if (value instanceof Number) {
            condition.add(name, Number.class.cast(value).doubleValue());
            return;
        }
        if (value instanceof JsonArray) {
            condition.add(name, JsonArray.class.cast(value));
            return;
        }
        condition.add(name, Value.of(value).get(String.class));
    }

    private JsonArray getArray(Object value) {
        List<Object> items = Value.of(value).get(new TypeReference<List<Object>>() {
        });
        return Json.createArrayBuilder(items).build();
    }

}
