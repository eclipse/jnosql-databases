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
package org.eclipse.jnosql.communication.arangodb.document;

import jakarta.nosql.Sort;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentQuery;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

final class QueryAQLConverter {

    private static final String FILTER = " FILTER ";
    private static final String LIMIT = " LIMIT ";
    private static final String IN = " IN ";
    private static final String SORT = " SORT ";
    private static final String REMOVE = " REMOVE ";
    private static final String RETURN = " RETURN ";
    private static final String SEPARATOR = " ";
    private static final String AND = " AND ";
    private static final String OR = " OR ";
    private static final String EQUALS = " == ";
    private static final String GREATER_EQUALS_THAN = " >= ";
    private static final String GREATER_THAN = " > ";
    private static final String LESSER_THAN = " < ";
    private static final String LESSER_EQUALS_THAN = " <= ";
    private static final String LIKE = " LIKE ";
    private static final String NOT = " NOT ";
    private static final char PARAM_APPENDER = '@';

    private QueryAQLConverter() {
    }

    public static AQLQueryResult delete(DocumentDeleteQuery query) throws NullPointerException {

        return convert(query.getDocumentCollection(),
                query.getCondition(),
                Collections.emptyList(),
                0L,
                0L,
                REMOVE, true);
    }

    public static AQLQueryResult select(DocumentQuery query) throws NullPointerException {

        return convert(query.getDocumentCollection(),
                query.getCondition(),
                query.getSorts(),
                query.getSkip(),
                query.getLimit(),
                RETURN, false);

    }


    private static AQLQueryResult convert(String documentCollection,
                                          Optional<DocumentCondition> documentCondition,
                                          List<Sort> sorts,
                                          long firstResult,
                                          long maxResult,
                                          String conclusion, boolean delete) {
        StringBuilder aql = new StringBuilder();
        Map<String, Object> params = new HashMap<>();
        char entity = Character.toLowerCase(documentCollection.charAt(0));
        aql.append("FOR ").append(entity).append(IN).append(documentCollection);

        documentCondition.ifPresent(condition -> {
            aql.append(FILTER);
            definesCondition(condition, aql, params, entity, 0);
        });
        if (!sorts.isEmpty()) {
            sort(sorts, aql, entity);
        }

        if (firstResult > 0 && maxResult > 0) {
            aql.append(LIMIT).append(firstResult)
                    .append(", ").append(maxResult);
        } else if (maxResult > 0) {
            aql.append(LIMIT).append(maxResult);
        }

        aql.append(conclusion).append(entity);
        if (delete) {
            aql.append(IN).append(documentCollection);
        }
        return new AQLQueryResult(aql.toString(), params);
    }

    private static void sort(List<Sort> sorts, StringBuilder aql, char entity) {
        aql.append(SORT);
        String separator = SEPARATOR;
        for (Sort sort : sorts) {
            aql.append(separator)
                    .append(entity).append('.')
                    .append(sort.getName())
                    .append(SEPARATOR).append(sort.getType());
            separator = " , ";
        }
    }

    private static void definesCondition(DocumentCondition condition,
                                         StringBuilder aql,
                                         Map<String, Object> params,
                                         char entity, int counter) {

        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case IN:
                appendCondition(aql, params, entity, document, IN);
                return;
            case EQUALS:
                appendCondition(aql, params, entity, document, EQUALS);
                return;
            case GREATER_EQUALS_THAN:
                appendCondition(aql, params, entity, document, GREATER_EQUALS_THAN);
                return;
            case GREATER_THAN:
                appendCondition(aql, params, entity, document, GREATER_THAN);
                return;
            case LESSER_THAN:
                appendCondition(aql, params, entity, document, LESSER_THAN);
                return;
            case LESSER_EQUALS_THAN:
                appendCondition(aql, params, entity, document, LESSER_EQUALS_THAN);
                return;
            case LIKE:
                appendCondition(aql, params, entity, document, LIKE);
                return;
            case AND:

                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {

                    if (isFirstCondition(aql, counter)) {
                        aql.append(AND);
                    }
                    definesCondition(dc, aql, params, entity, ++counter);
                }
                return;
            case OR:

                for (DocumentCondition dc : document.get(new TypeReference<List<DocumentCondition>>() {
                })) {
                    if (isFirstCondition(aql, counter)) {
                        aql.append(OR);
                    }
                    definesCondition(dc, aql, params, entity, ++counter);
                }
                return;
            case NOT:
                DocumentCondition documentCondition = document.get(DocumentCondition.class);
                aql.append(NOT);
                definesCondition(documentCondition, aql, params, entity, ++counter);
                return;
            default:
                throw new IllegalArgumentException("The condition does not support in AQL: " + condition.getCondition());
        }
    }

    private static boolean isFirstCondition(StringBuilder aql, int count) {
        return count > 0 && !FILTER.equals(aql.substring(aql.length() - 8));
    }

    private static void appendCondition(StringBuilder aql, Map<String, Object> params,
                                        char entity, Document document, String condition) {
        String nameParam = getNameParam(document.getName(), params);
        aql.append(SEPARATOR).append(entity).append('.').append(document.getName())
                .append(condition).append(PARAM_APPENDER).append(nameParam);
        if(IN.equals(condition)) {
            params.put(nameParam, ValueUtil.convertToList(document.getValue()));
        } else {
            params.put(nameParam, document.get());
        }
    }

    private static String getNameParam(String name, Map<String, Object> params) {
        String parameter = getNameParam(name);

        String paramName = parameter;
        int counter = 1;
        while (params.containsKey(paramName)) {
            paramName = parameter + '_' + counter;
        }

        return paramName;
    }

    private static String getNameParam(String name) {
        if ('_' == name.charAt(0)) {
            return name.substring(1);
        }
        return name;
    }


}
