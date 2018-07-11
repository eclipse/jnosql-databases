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
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
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
                selector.add(name, value.toString());
                return;
            case GREATER_THAN:
            case GREATER_EQUALS_THAN:
            case LESSER_THAN:
            case LESSER_EQUALS_THAN:
            case IN:
            case LIKE:
            case AND:
            case OR:
            case NOT:
            case BETWEEN:
            default:
                throw new UnsupportedOperationException("This operation is not supported at couchdb: " + condition.getCondition());

        }
    }
}
