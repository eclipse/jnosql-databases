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
package org.jnosql.diana.arangodb.document;

import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

final class AQLUtils {

    private AQLUtils() {
    }

    public static AQLQueryResult convert(DocumentQuery query) throws NullPointerException {
        StringBuilder aql = new StringBuilder();
        Map<String, Object> params = new HashMap<>();
        char entity = Character.toLowerCase(query.getDocumentCollection().charAt(0));
        aql.append("FOR ").append(entity).append(" IN ").append(query.getDocumentCollection());

        if (query.getCondition().isPresent()) {
            aql.append(" FILTER ");
            DocumentCondition condition = query.getCondition().get();
            definesCondition(condition, aql, params, entity, 0);
        }
        if (!query.getSorts().isEmpty()) {
            sort(query, aql, entity);
        }

        if (query.getFirstResult() > 0 && query.getMaxResults() > 0) {
            aql.append(" LIMIT ").append(query.getFirstResult())
                    .append(", ").append(query.getMaxResults());
        } else if (query.getMaxResults() > 0) {
            aql.append(" LIMIT ").append(query.getMaxResults());
        }

        aql.append(" RETURN ").append(entity);
        return new AQLQueryResult(aql.toString(), params);
    }

    private static void sort(DocumentQuery query, StringBuilder aql, char entity) {
        aql.append(" SORT ");
        String separator = " ";
        for (Sort sort : query.getSorts()) {
            aql.append(separator)
                    .append(entity).append('.')
                    .append(sort.getName())
                    .append(" ").append(sort.getType());
            separator = " , ";
        }
    }

    private static void definesCondition(DocumentCondition condition,
                                         StringBuilder aql,
                                         Map<String, Object> params,
                                         char entity, int count) {

        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case IN:
                appendCondtion(aql, params, entity, document, " IN ");
                return;
            case EQUALS:
                appendCondtion(aql, params, entity, document, " == ");
                return;
            case GREATER_EQUALS_THAN:
                appendCondtion(aql, params, entity, document, " >= ");
                return;
            case GREATER_THAN:
                appendCondtion(aql, params, entity, document, " > ");
                return;
            case LESSER_THAN:
                appendCondtion(aql, params, entity, document, " < ");
                return;
            case LESSER_EQUALS_THAN:
                appendCondtion(aql, params, entity, document, " <= ");
                return;
            case LIKE:
                appendCondtion(aql, params, entity, document, " LIKE ");
                return;
            case AND:

                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {
                    if (count > 0) {
                        aql.append(" AND ");
                    }
                    definesCondition(dc, aql, params, entity, ++count);
                }
                return;
            case OR:

                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {
                    if (count > 0) {
                        aql.append(" OR ");
                    }
                    definesCondition(dc, aql, params, entity, ++count);
                }
                return;
            case NOT:
                DocumentCondition documentCondition = document.get(DocumentCondition.class);
                aql.append(" NOT ");
                definesCondition(documentCondition, aql, params, entity, ++count);
                return;
        }
    }

    private static void appendCondtion(StringBuilder aql, Map<String, Object> params, char entity, Document document,
                                       String condition) {
        String nameParam = getNameParam(document.getName());
        aql.append(" ").append(entity).append('.').append(document.getName())
                .append(condition).append('@').append(nameParam);
        params.put(nameParam, document.get());
    }

    private static String getNameParam(String name) {
        if ('_' == name.charAt(0)) {
            return name.substring(1);
        }
        return name;
    }


}
