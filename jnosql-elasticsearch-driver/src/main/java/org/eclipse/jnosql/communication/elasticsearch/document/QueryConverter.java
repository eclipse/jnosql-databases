/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.elasticsearch.document;


import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.json.JsonData;
import jakarta.nosql.Condition;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentQuery;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.*;
import java.util.stream.Stream;

import static jakarta.nosql.Condition.EQUALS;
import static jakarta.nosql.Condition.IN;
import static org.eclipse.jnosql.communication.elasticsearch.document.EntityConverter.ENTITY;
import static org.eclipse.jnosql.communication.elasticsearch.document.EntityConverter.ID_FIELD;

final class QueryConverter {

    private static final Set<Condition> NOT_APPENDABLE = EnumSet.of(IN, Condition.AND, Condition.OR);

    private QueryConverter() {
    }

    static QueryConverterResult select(DocumentQuery query) {
        List<String> ids = new ArrayList<>();

        Query.Builder nameCondition = Optional.of(query.getDocumentCollection())
                .map(collection -> new Query.Builder().term(q -> q
                        .field(ENTITY).value(collection)))
                .map(Query.Builder.class::cast)
                .orElse(null);

        Query.Builder queryConditions = query.getCondition()
                .map(c -> getCondition(c, ids))
                .orElse(null);


        Query.Builder builder = Stream.of(nameCondition, queryConditions)
                .filter(Objects::nonNull)
                .reduce((c1, c2) -> (Query.Builder) new Query.Builder().bool(BoolQuery.of(b -> b
                        .must(c1.build(), c2.build()))))
                .orElse(null);

        return new QueryConverterResult(builder, ids);

    }


    private static Query.Builder getCondition(DocumentCondition condition, List<String> ids) {
        Document document = condition.getDocument();

        if (!NOT_APPENDABLE.contains(condition.getCondition()) && isIdField(document)) {
            if (IN.equals(condition.getCondition())) {
                ids.addAll(document.get(new TypeReference<List<String>>() {
                }));
            } else if (EQUALS.equals(condition.getCondition())) {
                ids.add(document.get(String.class));
            }

            return null;
        }

        switch (condition.getCondition()) {
            case EQUALS:
                return (Query.Builder) new Query.Builder()
                        .term(TermQuery.of(tq -> tq
                                .field(document.getName())
                                .value(v -> v
                                        .anyValue(JsonData.of(document.getValue().get())))));
            case LESSER_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.getName())
                                .lt(JsonData.of(document.getValue().get()))));
            case LESSER_EQUALS_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.getName())
                                .lte(JsonData.of(document.getValue().get()))));
            case GREATER_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.getName())
                                .gt(JsonData.of(document.getValue().get()))));
            case GREATER_EQUALS_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.getName())
                                .gte(JsonData.of(document.getValue().get()))));
            case LIKE:
                return (Query.Builder) new Query.Builder()
                        .queryString(QueryStringQuery.of(rq -> rq
                                .query(document.getValue().get(String.class))
                                .allowLeadingWildcard(true)
                                .fields(document.getName())));
            case IN:
                return (Query.Builder) new Query.Builder()
                        .term(TermQuery.of(tq -> tq
                                .field(document.getName())
                                .value(v -> v
                                        .anyValue(JsonData.of(ValueUtil.convertToList(document.getValue()))))));
            case AND:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .stream()
                        .map(d -> getCondition(d, ids))
                        .filter(Objects::nonNull)
                        .reduce((d1, d2) -> (Query.Builder) new Query.Builder()
                                .bool(BoolQuery.of(bq -> bq
                                        .must(List.of(d1.build(), d2.build()))))
                        ).orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));

            case OR:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .stream()
                        .map(d -> getCondition(d, ids))
                        .filter(Objects::nonNull)
                        .reduce((d1, d2) -> (Query.Builder) new Query.Builder()
                                .bool(BoolQuery.of(bq -> bq
                                        .should(List.of(d1.build(), d2.build())))))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));
            case NOT:
                DocumentCondition dc = document.get(DocumentCondition.class);
                return (Query.Builder) new Query.Builder()
                        .bool(BoolQuery.of(bq -> bq
                                .mustNot(getCondition(dc, ids).build())));
            default:
                throw new IllegalStateException("This condition is not supported at elasticsearch: " + condition.getCondition());
        }
    }

    private static boolean isIdField(Document document) {
        return ID_FIELD.equals(document.getName());
    }


}
