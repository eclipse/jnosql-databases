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

package org.jnosql.diana.ravendb.document;

import net.ravendb.client.documents.queries.Query;
import net.ravendb.client.documents.session.IDocumentQuery;
import net.ravendb.client.documents.session.IDocumentSession;
import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;

class DocumentQueryConversor {

    private DocumentQueryConversor() {
    }


    public static QueryResult createQuery(IDocumentSession session, DocumentQuery query) {

        List<String> ids = new ArrayList<>();
        IDocumentQuery<HashMap> ravenQuery = session.query(HashMap.class, Query.collection(query.getDocumentCollection()));
        query.getCondition().ifPresent(c -> feedQuery(ravenQuery, c, ids));


        Consumer<Sort> sortConsumer = s -> {
            switch (s.getType()) {
                case DESC:
                    ravenQuery.orderByDescending(s.getName());
                    return;
                case ASC:
                default:
                    ravenQuery.orderBy(s.getName());
            }
        };
        query.getSorts().forEach(sortConsumer);
        return new QueryResult(ids, ravenQuery);
    }

    private static void feedQuery(IDocumentQuery<HashMap> ravenQuery, DocumentCondition condition, List<String> ids) {
        Document document = condition.getDocument();
        Object value = document.get();
        String name = document.getName();

        if(EntityConverter.ID_FIELD.equals(name)) {
            ids.add(value.toString());
        }

        switch (condition.getCondition()) {
            case EQUALS:
                ravenQuery.whereEquals(name, value);
                return;
            case GREATER_THAN:
                ravenQuery.whereGreaterThan(name, value);
                return;
            case GREATER_EQUALS_THAN:
                ravenQuery.whereGreaterThanOrEqual(name, value);
                return;
            case LESSER_THAN:
                ravenQuery.whereLessThan(name, value);
                return;
            case LESSER_EQUALS_THAN:
                ravenQuery.whereLessThanOrEqual(name, value);
                return;
            case IN:
                ravenQuery.whereIn(name, document.get(new TypeReference<List<Object>>() {
                }));
                return;
            case LIKE:
                throw new UnsupportedOperationException("Raven does not support LIKE Operator");
            case AND:
                condition.getDocument().getValue()
                        .get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .forEach(c -> feedQuery(ravenQuery.andAlso(), c, ids));
                return;
            case OR:
                condition.getDocument().getValue()
                        .get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .forEach(c -> feedQuery(ravenQuery.orElse(), c, ids));
                return;
            default:
                throw new UnsupportedOperationException("The condition " + condition.getCondition()
                        + " is not supported from mongoDB diana driver");
        }
    }


    static class QueryResult {

        private final List<String> ids;

        private final IDocumentQuery<HashMap> ravenQuery;

        private QueryResult(List<String> ids, IDocumentQuery<HashMap> ravenQuery) {
            this.ids = ids;
            this.ravenQuery = ravenQuery;
        }

        public List<String> getIds() {
            return ids;
        }

        public IDocumentQuery<HashMap> getRavenQuery() {
            return ravenQuery;
        }
    }
}

