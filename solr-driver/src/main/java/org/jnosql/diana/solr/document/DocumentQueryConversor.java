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

package org.jnosql.diana.solr.document;


import jakarta.nosql.TypeReference;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentCondition;
import org.jnosql.diana.driver.ValueUtil;

import java.util.List;
import java.util.stream.Collectors;

final class DocumentQueryConversor {

    private DocumentQueryConversor() {
    }

    public static String convert(DocumentCondition condition) {
        Document document = condition.getDocument();
        Object value = ValueUtil.convert(document.getValue());

        switch (condition.getCondition()) {
            case EQUALS:
            case LIKE:
                return document.getName() + ':' + value;
            case GREATER_EQUALS_THAN:
            case GREATER_THAN:
                return document.getName() + ":[" + value + " TO *]";
            case LESSER_EQUALS_THAN:
            case LESSER_THAN:
                return document.getName() + ":[* TO " + value + "]";
            case IN:
                final String inConditions = ValueUtil.convertToList(document.getValue())
                        .stream()
                        .map(Object::toString).collect(Collectors.joining(" OR "));
                return document.getName() + ":(" + inConditions + ')';
            case NOT:
                return '-' + convert(document.get(DocumentCondition.class));

            case AND:
                return getDocumentConditions(condition).stream()
                        .map(DocumentQueryConversor::convert)
                        .collect(Collectors.joining(" AND "));
            case OR:
                return getDocumentConditions(condition).stream()
                        .map(DocumentQueryConversor::convert)
                        .collect(Collectors.joining(" OR "));
            default:
                throw new UnsupportedOperationException("The condition " + condition.getCondition()
                        + " is not supported from mongoDB diana driver");
        }
    }

    private static List<DocumentCondition> getDocumentConditions(DocumentCondition condition) {
        return condition.getDocument().getValue().get(new TypeReference<List<DocumentCondition>>() {
        });
    }

}
