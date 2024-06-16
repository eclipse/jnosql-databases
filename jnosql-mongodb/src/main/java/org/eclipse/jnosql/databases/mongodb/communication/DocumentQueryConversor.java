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

package org.eclipse.jnosql.databases.mongodb.communication;


import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.ValueUtil;
import org.eclipse.jnosql.communication.semistructured.CriteriaCondition;
import org.eclipse.jnosql.communication.semistructured.Element;

import java.util.List;

final class DocumentQueryConversor {

    private DocumentQueryConversor() {
    }

    public static Bson convert(CriteriaCondition condition) {
        Element document = condition.element();
        Object value = ValueUtil.convert(document.value());
        return switch (condition.condition()) {
            case EQUALS -> Filters.eq(document.name(), value);
            case GREATER_THAN -> Filters.gt(document.name(), value);
            case GREATER_EQUALS_THAN -> Filters.gte(document.name(), value);
            case LESSER_THAN -> Filters.lt(document.name(), value);
            case LESSER_EQUALS_THAN -> Filters.lte(document.name(), value);
            case IN -> {
                List<Object> inList = ValueUtil.convertToList(document.value());
                yield Filters.in(document.name(), inList.toArray());
            }
            case NOT -> Filters.not(convert(document.get(CriteriaCondition.class)));
            case LIKE -> Filters.regex(document.name(), value.toString());
            case AND -> {
                List<CriteriaCondition> andList = condition.element().value().get(new TypeReference<>() {
                });
                yield Filters.and(andList.stream()
                        .map(DocumentQueryConversor::convert).toList());
            }
            case OR -> {
                List<CriteriaCondition> orList = condition.element().value().get(new TypeReference<>() {
                });
                yield Filters.or(orList.stream()
                        .map(DocumentQueryConversor::convert).toList());
            }case BETWEEN -> {
                List<Object> betweenList = ValueUtil.convertToList(document.value());
                yield Filters.and(Filters.gte(document.name(), betweenList.get(0)),
                        Filters.lte(document.name(), betweenList.get(1)));

            }
            default -> throw new UnsupportedOperationException("The condition " + condition.condition()
                    + " is not supported from mongoDB diana driver");
        };
    }


}
