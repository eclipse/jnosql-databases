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
import com.couchbase.client.java.query.Statement;
import com.couchbase.client.java.query.dsl.Expression;
import com.couchbase.client.java.query.dsl.path.MutateLimitPath;
import jakarta.nosql.Condition;
import jakarta.nosql.Sort;
import jakarta.nosql.SortType;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentQuery;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.couchbase.client.java.query.dsl.Expression.x;
import static java.util.Objects.nonNull;
import static jakarta.nosql.Condition.EQUALS;
import static jakarta.nosql.Condition.IN;
import static org.jnosql.diana.couchbase.document.EntityConverter.ID_FIELD;
import static org.jnosql.diana.couchbase.document.EntityConverter.KEY_FIELD;
import static org.jnosql.diana.couchbase.document.StatementFactory.create;

final class QueryConverter {

    private static final Set<Condition> NOT_APPENDABLE = EnumSet.of(IN, Condition.AND, Condition.OR);

    private static final char PARAM_PREFIX = '$';
    private static final String[] ALL_SELECT = {"*"};
    private static final Function<Sort, com.couchbase.client.java.query.dsl.Sort> SORT_MAP = s -> {
        if (SortType.ASC.equals(s.getType())) {
            return com.couchbase.client.java.query.dsl.Sort.asc(s.getName());
        } else {
            return com.couchbase.client.java.query.dsl.Sort.desc(s.getName());
        }
    };

    private QueryConverter() {
    }

    static QueryConverterResult select(DocumentQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        List<String> keys = new ArrayList<>();
        String[] documents = query.getDocuments().stream().toArray(String[]::new);
        if (documents.length == 0) {
            documents = ALL_SELECT;
        }

        Statement statement = null;
        int skip = (int) query.getSkip();
        int limit = (int) query.getLimit();

        com.couchbase.client.java.query.dsl.Sort[] sorts = query.getSorts().stream().map(SORT_MAP).
                toArray(com.couchbase.client.java.query.dsl.Sort[]::new);

        if (query.getCondition().isPresent()) {
            Expression condition = getCondition(query.getCondition().get(), params, keys, query.getDocumentCollection());
            if (nonNull(condition)) {
                statement = create(bucket, documents, skip, limit, sorts, condition);
            } else {
                statement = null;
            }
        } else {
            statement = create(bucket, documents, skip, limit, sorts);
        }
        return new QueryConverterResult(params, statement, keys);
    }


    static QueryConverterResult delete(DocumentDeleteQuery query, String bucket) {
        JsonObject params = JsonObject.create();
        List<String> ids = new ArrayList<>();
        Expression condition = getCondition(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condigtion is required")), params, ids,
                query.getDocumentCollection());
        MutateLimitPath statement = null;
        if (nonNull(condition)) {
            statement = Delete.deleteFrom(bucket).where(condition);
        }

        return new QueryConverterResult(params, statement, ids);
    }

    private static Expression getCondition(DocumentCondition condition, JsonObject params
            , List<String> keys, String documentCollection) {
        Document document = condition.getDocument();

        if (!NOT_APPENDABLE.contains(condition.getCondition()) && isKeyField(document)) {
            if (IN.equals(condition.getCondition())) {
                inKeys(keys, documentCollection, document);
            } else if (EQUALS.equals(condition.getCondition())) {
                eqKeys(keys, documentCollection, document);

            }

            return null;
        }
        if (!NOT_APPENDABLE.contains(condition.getCondition())) {
            if(Condition.BETWEEN.equals(condition.getCondition()) || Condition.IN.equals(condition.getCondition())) {
                params.put(document.getName(), ValueUtil.convertToList(document.getValue()));
            } else {
                params.put(document.getName(), ValueUtil.convert(document.getValue()));
            }
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
                return x(document.getName()).in(x(PARAM_PREFIX + document.getName()));
            case BETWEEN:
                return x(document.getName()).between(x(PARAM_PREFIX + document.getName()));
            case AND:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params, keys, documentCollection))
                        .filter(Objects::nonNull)
                        .reduce(Expression::and)
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));


            case OR:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, params, keys, documentCollection))
                        .reduce(Expression::or)
                        .orElseThrow(() -> new IllegalStateException("An or condition cannot be empty"));
            case NOT:
                DocumentCondition dc = document.get(DocumentCondition.class);
                return getCondition(dc, params, keys, documentCollection).not();
            default:
                throw new IllegalStateException("This condition is not supported at coubhbase: " + condition.getCondition());
        }
    }

    private static void eqKeys(List<String> keys, String documentCollection, Document document) {
        if(document.getName().equals(KEY_FIELD)){
            keys.add(document.get(String.class));
        } else {
            keys.add(EntityConverter.getPrefix(documentCollection, document.get(String.class)));
        }
    }

    private static void inKeys(List<String> keys, String documentCollection, Document document) {
        if(document.getName().equals(KEY_FIELD)){
            keys.addAll(document.get(new TypeReference<List<String>>() {
            }));
        } else {
            List<String> ids = document.get(new TypeReference<List<String>>() {});
            ids.stream().map(id -> EntityConverter.getPrefix(documentCollection, id))
                    .forEach(keys::add);
        }
    }

    private static boolean isKeyField(Document document) {
        return ID_FIELD.equals(document.getName()) || KEY_FIELD.equals(document.getName());
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
