/*
 *  Copyright (c) 2024 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import oracle.nosql.driver.values.FieldValue;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.JSON_FIELD;

abstract class AbstractQueryBuilder implements Supplier<OracleQuery> {

    static final int ORIGIN = 0;
    private final String table;

    AbstractQueryBuilder(String table) {
        this.table = table;
    }

    protected void condition(DocumentCondition condition, StringBuilder query, List<FieldValue> params, List<String> ids) {
        Document document = condition.document();
        switch (condition.condition()) {
            case EQUALS:
                if (document.name().equals(DefaultOracleDocumentManager.ID)) {
                    ids.add(document.get(String.class));
                } else {
                    predicate(query, " = ", document, params);
                }
                return;
            case IN:
                if (document.name().equals(DefaultOracleDocumentManager.ID)) {
                    ids.addAll(document.get(new TypeReference<List<String>>() {
                    }));
                } else {
                    predicate(query, " IN ", document, params);
                }
                return;
            case LESSER_THAN:
                predicate(query, " < ", document, params);
                return;
            case GREATER_THAN:
                predicate(query, " > ", document, params);
                return;
            case LESSER_EQUALS_THAN:
                predicate(query, " <= ", document, params);
                return;
            case GREATER_EQUALS_THAN:
                predicate(query, " >= ", document, params);
                return;
            case LIKE:
                predicate(query, " LIKE ", document, params);
                return;
            case NOT:
                query.append(" NOT ");
                condition(document.get(DocumentCondition.class), query, params, ids);
                return;
            case OR:
                appendCondition(query, params, document.get(new TypeReference<>() {
                }), " OR ", ids);
                return;
            case AND:
                appendCondition(query, params, document.get(new TypeReference<>() {
                }), " AND ", ids);
                return;
            case BETWEEN:
                predicateBetween(query, params, document);
                return;
            default:
                throw new UnsupportedOperationException("There is not support condition for " + condition.condition());
        }
    }

    protected void predicateBetween(StringBuilder query,List<FieldValue> params, Document document) {
        query.append(" BETWEEN ");
        String name = identifierOf(document.name());

        List<Object> values = new ArrayList<>();
        ((Iterable<?>) document.get()).forEach(values::add);

        query.append(name).append(" ? AND  ? ");
        FieldValue fieldValue = FieldValueConverter.INSTANCE.of(values.get(ORIGIN));
        FieldValue fieldValue2 = FieldValueConverter.INSTANCE.of(values.get(1));
        params.add(fieldValue);
        params.add(fieldValue2);
    }

    protected void appendCondition(StringBuilder query, List<FieldValue> params,
                                 List<DocumentCondition> conditions,
                                 String condition, List<String> ids) {
        int index = ORIGIN;
        for (DocumentCondition documentCondition : conditions) {
            StringBuilder appendQuery = new StringBuilder();
            condition(documentCondition, appendQuery, params, ids);
            if(index == ORIGIN && !appendQuery.isEmpty()){
                query.append(appendQuery);
            } else if(!appendQuery.isEmpty()) {
                if(!query.substring(query.length()-condition.length()).equals(condition)){
                    query.append(condition);
                }
                query.append(appendQuery);
            }
            index++;
        }
    }

    protected void predicate(StringBuilder query,
                           String condition,
                           Document document,
                           List<FieldValue> params) {
        String name = identifierOf(document.name());
        Object value = document.get();
        FieldValue fieldValue = FieldValueConverter.INSTANCE.of(value);
        if(fieldValue.isArray()){
            query.append(name).append(condition).append(" ?[] ");
        } else {
            query.append(name).append(condition).append(" ? ");
        }
        params.add(fieldValue);
    }

    protected String identifierOf(String name) {
        return ' ' + table + "." + JSON_FIELD + "." + name + ' ';
    }

    protected void entityCondition(StringBuilder query, String tableName) {
        query.append(" WHERE ").append(table).append(".entity= '").append(tableName).append("'");
    }
}
