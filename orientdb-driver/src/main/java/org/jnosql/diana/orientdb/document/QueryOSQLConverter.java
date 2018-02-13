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

import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.List;

final class QueryOSQLConverter {


    private QueryOSQLConverter() {

    }

    static Query select(DocumentQuery documentQuery) {
        StringBuilder query = new StringBuilder();
        List<Object> params = new java.util.ArrayList<>();
        query.append("SELECT FROM ");
        query.append(documentQuery.getDocumentCollection());
        int counter = 0;
        if (documentQuery.getCondition().isPresent()) {
            query.append(" WHERE ");
            counter = addCondition(documentQuery.getCondition().get(), counter, query, params);
        }
        return new Query(query.toString(), params);
    }

    private static int addCondition(DocumentCondition documentCondition, int counter, StringBuilder query, List<Object> params) {
        return addCondition(documentCondition, counter, query, params, "AND");
    }

    private static int addCondition(DocumentCondition documentCondition, int counter, StringBuilder query, List<Object> params, String connector) {
        Document document = documentCondition.getDocument();
        Condition condition = documentCondition.getCondition();
        int aux = counter;
        if (Condition.AND.equals(condition) || Condition.OR.equals(condition) || Condition.NOT.equals(condition)) {
            List<DocumentCondition> documentConditions = document.get(new TypeReference<List<DocumentCondition>>() {
            });

            for (DocumentCondition dc : documentConditions) {
                aux = addCondition(dc, aux, query, params, toCondition(condition));
            }
            return aux;

        }
        if (counter > 0) {
            query.append(' ').append(connector).append(' ');
        }
        query.append(document.getName())
                .append(' ')
                .append(toCondition(condition))
                .append(' ')
                .append('?');
        params.add(document.get());
        return aux + 1;
    }

    private static String toCondition(Condition condition) {
        switch (condition) {
            case AND:
                return "AND";
            case EQUALS:
                return "=";
            case GREATER_EQUALS_THAN:
                return ">=";
            case GREATER_THAN:
                return ">";
            case IN:
                return "IN";
            case LESSER_EQUALS_THAN:
                return "<=";
            case LESSER_THAN:
                return "<";
            case LIKE:
                return "LIKE";
            case NOT:
                return "NOT";
            case OR:
                return "OR";
            default:
                throw new IllegalArgumentException("Orient DB has not support to the condition " + condition);

        }
    }

    static class Query {
        private final String query;
        private final List<Object> params;

        Query(String query, List<Object> params) {
            this.query = query;
            this.params = params;
        }

        String getQuery() {
            return query;
        }

        List<Object> getParams() {
            return params;
        }
    }

}
