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

import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.function.Function;

final class MangoQueryConverter implements Function<DocumentQuery, JsonObject> {


    @Override
    public JsonObject apply(DocumentQuery documentQuery) {
        JsonObject selector = getSelector(documentQuery);
        return Json.createObjectBuilder().add("selector", selector).build();
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
