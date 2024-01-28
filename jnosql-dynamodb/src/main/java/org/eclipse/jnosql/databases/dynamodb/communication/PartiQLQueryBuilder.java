/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.joining;
import static org.eclipse.jnosql.databases.dynamodb.communication.DocumentEntityConverter.toAttributeValue;

abstract class PartiQLQueryBuilder implements Supplier<PartiQLQuery> {

    protected void condition(DocumentCondition condition,
                             StringBuilder query,
                             List<AttributeValue> params) {
        Document document = condition.document();

        switch (condition.condition()) {
            case EQUALS:
                predicate(query, " = ", document, params);
                break;
            case LIKE:
                predicateLike(query, document, params);
                break;
            case IN:
                predicateIn(query, document, params);
                break;
            case GREATER_THAN:
                predicate(query, " > ", document, params);
                break;
            case LESSER_THAN:
                predicate(query, " >= ", document, params);
                break;
            case GREATER_EQUALS_THAN:
                predicate(query, " < ", document, params);
                break;
            case LESSER_EQUALS_THAN:
                predicate(query, " <= ", document, params);
                break;
            case BETWEEN:
                predicateBetween(query, document, params);
                break;
            case AND:
                appendCondition(query, params, document.get(new TypeReference<>() {
                }), " AND ");
                break;
            case OR:
                appendCondition(query, params, document.get(new TypeReference<>() {
                }), " OR ");
                break;
            case NOT:
                query.append(" NOT ");
                condition(document.get(DocumentCondition.class), query, params);
                break;
            default:
                throw new IllegalArgumentException("Unknown condition " + condition.condition());
        }
    }

    private void predicateIn(StringBuilder query, Document document, List<AttributeValue> params) {
        var name = document.name();
        List<Object> values = new ArrayList<>();
        ((Iterable<?>) document.get()).forEach(values::add);
        query.append(name).append(" IN (")
                .append(values.stream().map(i->"?").collect(joining(",")))
                .append(") ");
        values.stream().map(DocumentEntityConverter::toAttributeValue).forEach(params::add);
    }

    protected void predicateLike(StringBuilder query, Document document, List<AttributeValue> params) {
        var name = document.name();
        var param = toAttributeValue(document.get());
        query.append("begins_with(").append(name).append(", ? )");
        params.add(param);
    }

    protected void predicateBetween(StringBuilder query, Document document, List<AttributeValue> params) {
        var name = document.name();
        List<Object> values = new ArrayList<>();
        ((Iterable<?>) document.get()).forEach(values::add);
        query.append(name).append(" BETWEEN ? AND ? ");
        params.add(toAttributeValue(values.get(0)));
        params.add(toAttributeValue(values.get(1)));
    }

    protected void appendCondition(StringBuilder query, List<AttributeValue> params, List<DocumentCondition> conditions, String condition) {
        boolean isFirstCondition = true;
        for (DocumentCondition documentCondition : conditions) {
            StringBuilder appendQuery = new StringBuilder();
            condition(documentCondition, appendQuery, params);
            if (isFirstCondition && !appendQuery.isEmpty()) {
                query.append(appendQuery);
            } else if (!appendQuery.isEmpty()) {
                if (!query.substring(query.length() - condition.length()).equals(condition)) {
                    query.append(condition);
                }
                query.append(appendQuery);
            }
            isFirstCondition = false;
        }

    }

    protected void predicate(StringBuilder query, String condition, Document document, List<AttributeValue> params) {
        var name = document.name();
        var param = toAttributeValue(document.get());
        query.append(name).append(condition).append(" ? ");
        params.add(param);
    }
}
