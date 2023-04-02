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
 *   Maximillian Arruda
 */
package org.eclipse.jnosql.databases.elasticsearch.communication;


import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch._types.query_dsl.QueryStringQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.RangeQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.TermQuery;
import co.elastic.clients.json.JsonData;
import org.eclipse.jnosql.communication.Condition;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static org.eclipse.jnosql.communication.Condition.EQUALS;
import static org.eclipse.jnosql.communication.Condition.IN;

final class QueryConverter {

    private static final Set<Condition> NOT_APPENDABLE = EnumSet.of(IN, Condition.AND, Condition.OR);

    private QueryConverter() {
    }

    static QueryConverterResult select(DocumentQuery query) {
        List<String> ids = new ArrayList<>();

        Query.Builder nameCondition = Optional.of(query.name())
                .map(collection -> new Query.Builder().term(q -> q
                        .field(EntityConverter.ENTITY).value(collection)))
                .map(Query.Builder.class::cast)
                .orElse(null);

        Query.Builder queryConditions = query.condition()
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
        Document document = condition.document();

        if (!NOT_APPENDABLE.contains(condition.condition()) && isIdField(document)) {
            if (IN.equals(condition.condition())) {
                ids.addAll(document.get(new TypeReference<List<String>>() {
                }));
            } else if (EQUALS.equals(condition.condition())) {
                ids.add(document.get(String.class));
            }

            return null;
        }

        switch (condition.condition()) {
            case EQUALS:
                return (Query.Builder) new Query.Builder()
                        .term(TermQuery.of(tq -> tq
                                .field(document.name())
                                .value(v -> v
                                        .anyValue(JsonData.of(document.value().get())))));
            case LESSER_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.name())
                                .lt(JsonData.of(document.value().get()))));
            case LESSER_EQUALS_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.name())
                                .lte(JsonData.of(document.value().get()))));
            case GREATER_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.name())
                                .gt(JsonData.of(document.value().get()))));
            case GREATER_EQUALS_THAN:
                return (Query.Builder) new Query.Builder()
                        .range(RangeQuery.of(rq -> rq
                                .field(document.name())
                                .gte(JsonData.of(document.value().get()))));
            case LIKE:
                return (Query.Builder) new Query.Builder()
                        .queryString(QueryStringQuery.of(rq -> rq
                                .query(document.value().get(String.class))
                                .allowLeadingWildcard(true)
                                .fields(document.name())));
            case IN:
                return (Query.Builder) ValueUtil.convertToList(document.value())
                        .stream()
                        .map(val -> new Query.Builder()
                                .term(TermQuery.of(tq -> tq
                                        .field(document.name())
                                        .value(v -> v.anyValue(JsonData.of(val))))))
                        .reduce((d1, d2) -> new Query.Builder()
                                .bool(BoolQuery.of(bq -> bq
                                        .should(List.of(d1.build(), d2.build())))))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));
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
                Query.Builder queryBuilder = Optional.ofNullable(getCondition(dc, ids))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));
                return (Query.Builder) new Query.Builder()
                        .bool(BoolQuery.of(bq -> bq
                                .mustNot(queryBuilder.build())));
            default:
                throw new IllegalStateException("This condition is not supported at elasticsearch: " + condition.condition());
        }
    }

    private static boolean isIdField(Document document) {
        return EntityConverter.ID_FIELD.equals(document.name());
    }


}
