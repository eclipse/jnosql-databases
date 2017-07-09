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
package org.jnosql.diana.couchbase.document;

import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.query.Delete;
import com.couchbase.client.java.query.Select;
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.MutateLimitPath;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.couchbase.client.java.query.dsl.Expression.i;
import static com.couchbase.client.java.query.dsl.Expression.x;
import static java.util.Objects.nonNull;
import static org.jnosql.diana.api.Condition.EQUALS;
import static org.jnosql.diana.api.Condition.IN;
import static org.jnosql.diana.couchbase.document.EntityConverter.KEY_FIELD;

final class QueryConverter {

    private static final Set<Condition> NOT_APPENDABLE = EnumSet.of(IN, Condition.AND, Condition.OR);

    private static final char PARAM_PREFIX = '$';
    private static final String[] ALL_SELECT = {"*"};

    private QueryConverter() {
    }

    static QueryConverterResult select(DocumentQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        List<String> keys = new ArrayList<>();
        String[] documents = query.getDocuments().stream().toArray(size -> new String[size]);
        if (documents.length == 0) {
            documents = ALL_SELECT;
        }
        Expression condition = getCondition(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required")), params, keys);
        Statement statement = null;
        if (nonNull(condition)) {
            statement = Select.select(documents).from(i(bucket))
                    .where(condition);
        }

        return new QueryConverterResult(params, statement, keys);
    }

    static QueryConverterResult delete(DocumentDeleteQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        List<String> ids = new ArrayList<>();
        Expression condition = getCondition(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condigtion is required")), params, ids);
        MutateLimitPath statement = null;
        if (nonNull(condition)) {
            statement = Delete.deleteFrom(bucket).where(condition);
        }

        return new QueryConverterResult(params, statement, ids);
    }

    private static Expression getCondition(DocumentCondition condition, JsonObject params, List<String> keys) {
        Document document = condition.getDocument();

        if (!NOT_APPENDABLE.contains(condition.getCondition()) && isKeyField(document)) {
            if (IN.equals(condition.getCondition())) {
                keys.addAll(document.get(new TypeReference<List<String>>() {
                }));
            } else if (EQUALS.equals(condition.getCondition())) {
                keys.add(document.get(String.class));
            }

            return null;
        }
        if (!NOT_APPENDABLE.contains(condition.getCondition())) {
            params.put(document.getName(), document.get());
        }

        switch (condition.getCondition()) {
            case EQUALS:
                return x(document.getName()).eq(x(PARAM_PREFIX + document.getName()));
            case LESSER_THAN:
                return x(document.getName()).lt(x(PARAM_PREFIX + document.getName()));
            case LESSER_EQUALS_THAN:
                return x(document.getName()).lte(x(PARAM_PREFIX + document.getName()));
            case GREATER_THAN:
                return x(document.getName()).gt(x(PARAM_PREFIX + document.getName()));
            case GREATER_EQUALS_THAN:
                return x(document.getName()).gte(x(PARAM_PREFIX + document.getName()));
            case LIKE:
                return x(document.getName()).like(x(PARAM_PREFIX + document.getName()));
            case IN:
                return x(document.getName()).like(x(PARAM_PREFIX + document.getName()));
            case AND:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params, keys))
                        .filter(Objects::nonNull)
                        .reduce((d1, d2) -> d1.and(d2))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));


            case OR:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params, keys))
                        .reduce((d1, d2) -> d1.or(d2))
                        .orElseThrow(() -> new IllegalStateException("An or condition cannot be empty"));
            case NOT:
                DocumentCondition dc = document.get(DocumentCondition.class);
                return getCondition(dc, params, keys).not();
            default:
                throw new IllegalStateException("This condition is not supported at coubhbase: " + condition.getCondition());
        }
    }

    private static boolean isKeyField(Document document) {
        return KEY_FIELD.equals(document.getName());
    }

    static class QueryConverterResult {

        private final JsonObject params;

        private final Statement statement;

        private final List<String> keys;

        QueryConverterResult(JsonObject params, Statement statement, List<String> keys) {
            this.params = params;
            this.statement = statement;
            this.keys = keys;
        }

        JsonObject getParams() {
            return params;
        }

        Statement getStatement() {
            return statement;
        }

        List<String> getKeys() {
            return keys;
        }
    }


}
