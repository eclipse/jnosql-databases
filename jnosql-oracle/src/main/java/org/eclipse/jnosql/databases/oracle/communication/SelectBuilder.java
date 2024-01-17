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

import jakarta.data.Direction;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.eclipse.jnosql.databases.oracle.communication.TableCreationConfiguration.JSON_FIELD;

final class SelectBuilder implements Supplier<OracleQuery> {

    private final DocumentQuery query;

    private final String table;

    SelectBuilder(DocumentQuery query, String table) {
        this.query = query;
        this.table = table;
    }

    @Override
    public OracleQuery get() {
        StringBuilder query = new StringBuilder();
        Map<String, Object> params = new HashMap<>();
        List<String> ids = new ArrayList<>();

        query.append("select ");
        query.append(select()).append(' ');
        query.append("from ").append(table);
        query.append(" WHERE " + JSON_FIELD + "._entity = '").append(this.query.name()).append("' ");
        this.query.condition().ifPresent(c -> condition(c, query, params, ids));


        if (!this.query.sorts().isEmpty()) {
            query.append(" ORDER BY ");
            String order = this.query.sorts().stream()
                    .map(s -> s.property() + " " + (s.isAscending() ? Direction.ASC : Direction.DESC))
                    .collect(Collectors.joining(", "));
            query.append(order);
        }

        if (this.query.limit() > 0) {
            query.append(" LIMIT ").append(this.query.limit());
        }

        if (this.query.skip() > 0) {
            query.append(" OFFSET ").append(this.query.skip());
        }
        return new OracleQuery(query.toString(), params, ids);
    }

    private String select() {
        String documents = query.documents().stream()
                .map(d -> JSON_FIELD + "." + d)
                .collect(Collectors.joining(", "));
        if (documents.isBlank()) {
            return "*";
        }
        return documents;
    }

    private void condition(DocumentCondition condition, StringBuilder query, Map<String, Object> params, List<String> ids) {
        Document document = condition.document();
        switch (condition.condition()) {
            case EQUALS:
                if (document.name().equals(OracleDocumentManager.ID)) {
                    ids.add(document.get(String.class));
                } else {
                    predicate(query, " = ", document, params);
                }
                return;
            case IN:
                if (document.name().equals(OracleDocumentManager.ID)) {
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

    private void predicateBetween(StringBuilder n1ql, Map<String, Object> params, Document document) {
        n1ql.append(" BETWEEN ");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String name = identifierOf(document.name());

        List<Object> values = new ArrayList<>();
        ((Iterable<?>) document.get()).forEach(values::add);

        String param = "$".concat(document.name()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        String param2 = "$".concat(document.name()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        n1ql.append(name).append(" ").append(param).append(" AND ").append(param2);
        params.put(param, values.get(0));
        params.put(param2, values.get(1));
    }

    private void appendCondition(StringBuilder query, Map<String, Object> params,
                                 List<DocumentCondition> conditions,
                                 String condition, List<String> ids) {
        int index = 0;
        for (DocumentCondition documentCondition : conditions) {
            StringBuilder appendQuery = new StringBuilder();
            condition(documentCondition, appendQuery, params, ids);
            if(index == 0){
                query.append(" ").append(appendQuery);
            } else {
                query.append(condition).append(appendQuery);
            }
            index++;
        }
    }

    private void predicate(StringBuilder query,
                           String condition,
                           Document document,
                           Map<String, Object> params) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String name = identifierOf(document.name());
        Object value = document.get();
        String param = "$".concat(document.name()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        query.append(name).append(condition).append(param);
        params.put(param, value);
    }

    private String identifierOf(String name) {
        return ' ' + JSON_FIELD + "." + name + ' ';
    }
}
