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


import com.orientechnologies.orient.core.id.ORecordId;
import jakarta.nosql.Sort;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.List;

final class QueryOSQLConverter {

    private static final String WHERE = " WHERE ";
    private static final String IN = " IN ";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private static final String EQUALS = " = ";
    private static final String GREATER_EQUALS_THAN = " >= ";
    private static final String GREATER_THAN = " > ";
    private static final String LESSER_THAN = " < ";
    private static final String LESSER_EQUALS_THAN = " <= ";
    private static final String LIKE = " LIKE ";
    private static final String SKIP = " SKIP ";
    private static final String LIMIT = " LIMIT ";
    private static final String SORT = " ORDER BY";
    private static final String SPACE = " ";
    private static final char PARAM_APPENDER = '?';

    private QueryOSQLConverter() {
    }

    static Query select(DocumentQuery documentQuery) {
        StringBuilder query = new StringBuilder();
        List<Object> params = new java.util.ArrayList<>();
        List<ORecordId> ids = new ArrayList<>();
        query.append("SELECT FROM ");
        query.append(documentQuery.getDocumentCollection());

        if (documentQuery.getCondition().isPresent()) {
            query.append(WHERE);
            definesCondition(documentQuery.getCondition().get(), query, params, 0, ids);
        }

        if (!documentQuery.getSorts().isEmpty()) {
            appendSort(documentQuery.getSorts(), query);
        }

        appendPagination(documentQuery, query);
        return new Query(query.toString(), params, ids);
    }

    private static void definesCondition(DocumentCondition condition, StringBuilder query, List<Object> params,
                                         int counter, List<ORecordId> ids) {

        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case IN:
                appendCondition(query, params, document, IN, ids);
                return;
            case EQUALS:
                appendCondition(query, params, document, EQUALS, ids);
                return;
            case GREATER_EQUALS_THAN:
                appendCondition(query, params, document, GREATER_EQUALS_THAN, ids);
                return;
            case GREATER_THAN:
                appendCondition(query, params, document, GREATER_THAN, ids);
                return;
            case LESSER_THAN:
                appendCondition(query, params, document, LESSER_THAN, ids);
                return;
            case LESSER_EQUALS_THAN:
                appendCondition(query, params, document, LESSER_EQUALS_THAN, ids);
                return;
            case LIKE:
                appendCondition(query, params, document, LIKE, ids);
                return;
            case AND:
                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {

                    if (isFirstCondition(query, counter)) {
                        query.append(AND);
                    }
                    definesCondition(dc, query, params, ++counter, ids);
                }
                return;
            case OR:
                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {
                    if (isFirstCondition(query, counter)) {
                        query.append(OR);
                    }
                    definesCondition(dc, query, params, ++counter, ids);
                }
                return;
            case NOT:
                DocumentCondition documentCondition = document.get(DocumentCondition.class);
                query.append("NOT (");
                definesCondition(documentCondition, query, params, ++counter, ids);
                query.append(")");
                return;
            default:
                throw new IllegalArgumentException("Orient DB has not support to the condition " + condition.getCondition());
        }
    }

    private static boolean isFirstCondition(StringBuilder query, int counter) {
        return counter > 0 && !WHERE.equals(query.substring(query.length() - 7));
    }

    private static void appendCondition(StringBuilder query, List<Object> params,
                                        Document document, String condition, List<ORecordId> ids) {

        if(OrientDBConverter.RID_FIELD.equals(document.getName())) {
            ids.add(new ORecordId(document.get(String.class)));
            return;
        }
        query.append(document.getName())
                .append(condition).append(PARAM_APPENDER);
        if(IN.equals(condition)) {
            params.add(ValueUtil.convertToList(document.getValue()));
        } else {
            params.add(document.get());
        }
    }

    private static void appendSort(List<Sort> sorts, StringBuilder query) {
        query.append(SORT);
        String separator = SPACE;
        for (Sort sort : sorts) {
            query.append(separator)
                    .append(sort.getName())
                    .append(SPACE)
                    .append(sort.getType());
            separator = ", ";
        }
    }

    private static void appendPagination(DocumentQuery documentQuery, StringBuilder query) {
        if (documentQuery.getSkip() > 0) {
            query.append(SKIP).append(documentQuery.getSkip());
        }

        if (documentQuery.getLimit() > 0) {
            query.append(LIMIT).append(documentQuery.getLimit());
        }
    }

    static class Query {
        private final String query;
        private final List<Object> params;
        private List<ORecordId> ids;

        Query(String query, List<Object> params,List<ORecordId> ids) {
            this.query = query;
            this.params = params;
            this.ids = ids;
        }

        String getQuery() {
            return query;
        }

        List<ORecordId> getIds() {
            return ids;
        }

        List<Object> getParams() {
            return params;
        }
    }
}
