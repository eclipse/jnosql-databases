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
package org.jnosql.diana.elasticsearch.document;


import jakarta.nosql.Condition;
import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import jakarta.nosql.document.DocumentQuery;
import org.elasticsearch.index.query.QueryBuilder;
import org.jnosql.diana.driver.ValueUtil;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static jakarta.nosql.Condition.EQUALS;
import static jakarta.nosql.Condition.IN;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.termsQuery;
import static org.jnosql.diana.elasticsearch.document.EntityConverter.ID_FIELD;

final class QueryConverter {

    private static final Set<Condition> NOT_APPENDABLE = EnumSet.of(IN, Condition.AND, Condition.OR);

    private QueryConverter() {
    }

    static QueryConverterResult select(DocumentQuery query) {
        List<String> ids = new ArrayList<>();
        return query.getCondition()
                .map(c -> getCondition(c, ids))
                .map(q -> new QueryConverterResult(q, ids))
                .orElse(new QueryConverterResult(null, ids));
    }


    private static QueryBuilder getCondition(DocumentCondition condition, List<String> ids) {
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
                return termQuery(document.getName(), ValueUtil.convert(document.getValue()));
            case LESSER_THAN:
                return rangeQuery(document.getName()).lt(ValueUtil.convert(document.getValue()));
            case LESSER_EQUALS_THAN:
                return rangeQuery(document.getName()).lte(ValueUtil.convert(document.getValue()));
            case GREATER_THAN:
                return rangeQuery(document.getName()).gt(ValueUtil.convert(document.getValue()));
            case GREATER_EQUALS_THAN:
                return rangeQuery(document.getName()).gte(ValueUtil.convert(document.getValue()));
            case LIKE:
                return matchQuery(document.getName(), ValueUtil.convert(document.getValue()));
            case IN:
                return termsQuery(document.getName(), ValueUtil.convertToList(document.getValue()));
            case AND:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, ids))
                        .filter(Objects::nonNull)
                        .reduce((d1, d2) -> boolQuery().filter(d1).filter(d2))
                        .orElseThrow(() -> new IllegalStateException("An and condition cannot be empty"));


            case OR:
                return document.get(new TypeReference<List<DocumentCondition>>() {
                })
                        .stream()
                        .map(d -> getCondition(d, ids))
                        .reduce((d1, d2) -> boolQuery().should(d1).should(d2))
                        .orElseThrow(() -> new IllegalStateException("An or condition cannot be empty"));
            case NOT:
                DocumentCondition dc = document.get(DocumentCondition.class);
                return boolQuery().mustNot(getCondition(dc, ids));
            default:
                throw new IllegalStateException("This condition is not supported at coubhbase: " + condition.getCondition());
        }
    }

    private static boolean isIdField(Document document) {
        return ID_FIELD.equals(document.getName());
    }


}
