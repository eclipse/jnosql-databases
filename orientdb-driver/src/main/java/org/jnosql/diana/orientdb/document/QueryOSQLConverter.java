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
 *   Lucas Furlaneto
 */
package org.jnosql.diana.orientdb.document;


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
        if (documentQuery.getCondition().isPresent()) {
            query.append(" WHERE ");
            definesCondition(documentQuery.getCondition().get(), query, params, 0);
        }
        return new Query(query.toString(), params);
    }

    private static void definesCondition(DocumentCondition condition, StringBuilder query, List<Object> params, int counter) {
        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case IN:
                appendCondition(query, params, document, " IN ");
                return;
            case EQUALS:
                appendCondition(query, params, document, " = ");
                return;
            case GREATER_EQUALS_THAN:
                appendCondition(query, params, document, " >= ");
                return;
            case GREATER_THAN:
                appendCondition(query, params, document, " > ");
                return;
            case LESSER_THAN:
                appendCondition(query, params, document, " < ");
                return;
            case LESSER_EQUALS_THAN:
                appendCondition(query, params, document, " <= ");
                return;
            case LIKE:
                appendCondition(query, params, document, " LIKE ");
                return;
            case AND:
                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {

                    if (isFirstCondition(query, counter)) {
                        query.append(" AND ");
                    }
                    definesCondition(dc, query, params, ++counter);
                }
                return;
            case OR:
                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {
                    if (isFirstCondition(query, counter)) {
                        query.append(" OR ");
                    }
                    definesCondition(dc, query, params, ++counter);
                }
                return;
            case NOT:
                DocumentCondition documentCondition = document.get(DocumentCondition.class);
                query.append("NOT (");
                definesCondition(documentCondition, query, params, ++counter);
                query.append(")");
                return;
            default:
                throw new IllegalArgumentException("Orient DB has not support to the condition " + condition.getCondition());
        }
    }

    private static boolean isFirstCondition(StringBuilder query, int counter) {
        return counter > 0 && !" WHERE ".equals(query.substring(query.length() - 7));
    }

    private static void appendCondition(StringBuilder query, List<Object> params, Document document, String condition) {
        query.append(document.getName())
                .append(condition).append("?");
        params.add(document.get());
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
