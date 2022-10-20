/*
 *  Copyright (c) 2022 Otávio Santana and others
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
package org.eclipse.jnosql.communication.couchbase.document;

import com.couchbase.client.java.json.JsonObject;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.eclipse.jnosql.communication.couchbase.document.EntityConverter.ID_FIELD;

final class N1QLBuilder implements Supplier<N1QLQuery> {

    private final DocumentQuery query;

    private final String database;

    private final String scope;

    private N1QLBuilder(DocumentQuery query, String database, String scope) {
        this.query = query;
        this.database = database;
        this.scope = scope;
    }

    @Override
    public N1QLQuery get() {
        StringBuilder n1ql = new StringBuilder();
        JsonObject params = JsonObject.create();
        List<String> ids = new ArrayList<>();

        n1ql.append("select ");
        n1ql.append(select()).append(' ');
        n1ql.append("from ")
                .append(database).append(".")
                .append(scope).append(".")
                .append(query.getDocumentCollection());

        query.getCondition().ifPresent(c -> {
            n1ql.append(" WHERE ");
            condition(c, n1ql, params, ids);
        });

        if (query.getLimit() > 0) {
            n1ql.append(" LIMIT ").append(query.getLimit());
        }

        if (query.getSkip() > 0) {
            n1ql.append(" OFFSET ").append(query.getSkip());
        }

        if (!query.getSorts().isEmpty()) {
            n1ql.append(" ORDER BY ");
            String order = query.getSorts().stream()
                    .map(s -> s.getName() + " " + s.getType().name())
                    .collect(Collectors.joining(", "));
            n1ql.append(order);
        }

        return N1QLQuery.of(n1ql, params, ids);
    }


    private void condition(DocumentCondition condition, StringBuilder n1ql, JsonObject params, List<String> ids) {
        Document document = condition.getDocument();
        switch (condition.getCondition()) {
            case EQUALS:
                if (document.getName().equals(ID_FIELD)) {
                    ids.add(document.get(String.class));
                } else {
                    predicate(n1ql, " = ", document, params);
                }
                return;
            case IN:
                if (document.getName().equals(ID_FIELD)) {
                    ids.addAll(document.get(new TypeReference<List<String>>() {
                    }));
                } else {
                    predicate(n1ql, " IN ", document, params);
                }
                return;
            case LESSER_THAN:
                predicate(n1ql, " < ", document, params);
                return;
            case GREATER_THAN:
                predicate(n1ql, " > ", document, params);
                return;
            case LESSER_EQUALS_THAN:
                predicate(n1ql, " <= ", document, params);
                return;
            case GREATER_EQUALS_THAN:
                predicate(n1ql, " >= ", document, params);
                return;
            case LIKE:
                predicate(n1ql, " LIKE ", document, params);
                return;
            case NOT:
                n1ql.append(" NOT ");
                condition(document.get(DocumentCondition.class), n1ql, params, ids);
                return;
            case OR:
                appendCondition(n1ql, params, document.get(new TypeReference<>() {
                }), " OR ", ids);
                return;
            case AND:
                appendCondition(n1ql, params, document.get(new TypeReference<>() {
                }), " AND ", ids);
                return;
            case BETWEEN:
                predicateBetween(n1ql, params, document);
                return;
            default:
                throw new UnsupportedOperationException("There is not support condition for " + condition.getCondition());
        }
    }

    private void predicateBetween(StringBuilder n1ql, JsonObject params, Document document) {
        n1ql.append(" BETWEEN ");
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String name = '\'' + document.getName() + '\'';

        List<Object> values = new ArrayList<>();
        ((Iterable<?>) document.get()).forEach(values::add);

        String param = "$".concat(document.getName()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        String param2 = "$".concat(document.getName()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        n1ql.append(name).append(" ").append(param).append(" AND ").append(param2);
        params.put(param, values.get(0));
        params.put(param2, values.get(1));
    }

    private void appendCondition(StringBuilder n1ql, JsonObject params,
                                 List<DocumentCondition> conditions,
                                 String condition, List<String> ids) {
        for (DocumentCondition documentCondition : conditions) {
            StringBuilder query = new StringBuilder();
            condition(documentCondition, query, params, ids);
            n1ql.append(condition).append(query);
        }
    }

    private void predicate(StringBuilder n1ql,
                           String condition,
                           Document document,
                           JsonObject params) {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        String name = '\'' + document.getName() + '\'';
        Object value = document.get();
        String param = "$".concat(document.getName()).concat("_").concat(Integer.toString(random.nextInt(0, 100)));
        n1ql.append(name).append(condition).append(param);
        params.put(param, value);
    }

    private String select() {
        String documents = query.getDocuments().stream()
                .collect(Collectors.joining(", "));
        if (documents.isBlank()) {
            return "*";
        }
        return documents;
    }

    public static N1QLBuilder of(DocumentQuery query, String database, String scope) {
        return new N1QLBuilder(query, database, scope);
    }
}
