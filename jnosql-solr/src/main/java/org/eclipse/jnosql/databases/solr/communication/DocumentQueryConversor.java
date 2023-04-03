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

package org.eclipse.jnosql.databases.solr.communication;


import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.List;
import java.util.stream.Collectors;

final class DocumentQueryConversor {

    private static final String SELECT_ALL_QUERY = "_entity:";

    private DocumentQueryConversor() {
    }


    static String convert(DocumentQuery query) {
        String rootCondition = SolrUtils.ENTITY + ':' + query.name();
        return rootCondition + query.condition()
                .map(DocumentQueryConversor::convert)
                .map(s -> " AND " + s).orElse("");
    }

    static String convert(DocumentDeleteQuery query) {
        String rootCondition = SolrUtils.ENTITY + ':' + query.name();
        return rootCondition + query.condition()
                .map(DocumentQueryConversor::convert)
                .map(s -> " AND " + s).orElse("");
    }

    private static String convert(DocumentCondition condition) {
        Document document = condition.document();
        Object value = ValueUtil.convert(document.value());

        switch (condition.condition()) {
            case EQUALS:
            case LIKE:
                return document.name() + ':' + value;
            case GREATER_EQUALS_THAN:
            case GREATER_THAN:
                return document.name() + ":[" + value + " TO *]";
            case LESSER_EQUALS_THAN:
            case LESSER_THAN:
                return document.name() + ":[* TO " + value + "]";
            case IN:
                final String inConditions = ValueUtil.convertToList(document.value())
                        .stream()
                        .map(Object::toString).collect(Collectors.joining(" OR "));
                return document.name() + ":(" + inConditions + ')';
            case NOT:
                return " NOT " + convert(document.get(DocumentCondition.class));

            case AND:
                return getDocumentConditions(condition).stream()
                        .map(DocumentQueryConversor::convert)
                        .collect(Collectors.joining(" AND "));
            case OR:
                return getDocumentConditions(condition).stream()
                        .map(DocumentQueryConversor::convert)
                        .collect(Collectors.joining(" OR "));
            default:
                throw new UnsupportedOperationException("The condition " + condition.condition()
                        + " is not supported from mongoDB diana driver");
        }
    }

    private static List<DocumentCondition> getDocumentConditions(DocumentCondition condition) {
        return condition.document().value().get(new TypeReference<>() {
        });
    }

}
