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

package org.eclipse.jnosql.communication.ravendb.document;

import jakarta.data.repository.Sort;
import org.eclipse.jnosql.communication.TypeReference;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import net.ravendb.client.documents.queries.Query;
import net.ravendb.client.documents.session.IDocumentQuery;
import net.ravendb.client.documents.session.IDocumentSession;
import org.eclipse.jnosql.communication.driver.ValueUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

class DocumentQueryConverter {

    private DocumentQueryConverter() {
    }


    public static QueryResult createQuery(IDocumentSession session, DocumentQuery query) {

        List<String> ids = new ArrayList<>();
        IDocumentQuery<HashMap> ravenQuery = session.query(HashMap.class, Query.collection(query.name()));
        query.condition().ifPresent(c -> feedQuery(ravenQuery, c, ids));

        if (!ids.isEmpty() && query.condition().isPresent()) {
            return new QueryResult(ids, null);
        } else {
            return appendRavenQuery(query, ids, ravenQuery);
        }

    }

    private static QueryResult appendRavenQuery(DocumentQuery query, List<String> ids, IDocumentQuery<HashMap> ravenQuery) {
        Consumer<Sort> sortConsumer = s -> {
            if(s.isDescending()){
                ravenQuery.orderByDescending(s.property());
            } else {
                ravenQuery.orderBy(s.property());
            }
        };
        query.sorts().forEach(sortConsumer);

        if (query.skip() > 0) {
            ravenQuery.skip((int) query.skip());
        }

        if (query.limit() > 0) {
            ravenQuery.take((int) query.limit());
        }
        return new QueryResult(ids, ravenQuery);
    }

    private static void feedQuery(IDocumentQuery<HashMap> ravenQuery, DocumentCondition condition, List<String> ids) {
        Document document = condition.document();
        Object value = document.get();
        String name = document.name();

        if (EntityConverter.ID_FIELD.equals(name)) {
            if (value instanceof Iterable) {
                final Iterable iterable = Iterable.class.cast(value);
                iterable.forEach(i -> ids.add(i.toString()));
            } else {
                ids.add(value.toString());
            }
            return;
        }

        switch (condition.condition()) {
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
                ravenQuery.whereIn(name, ValueUtil.convertToList(document.value()));
                return;
            case NOT:
                ravenQuery.negateNext();
                feedQuery(ravenQuery, document.get(DocumentCondition.class), ids);
                return;
            case LIKE:
                throw new UnsupportedOperationException("Raven does not support LIKE Operator");
            case AND:
                condition.document().value()
                        .get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .forEach(c -> feedQuery(ravenQuery.andAlso(), c, ids));
                return;
            case OR:
                condition.document().value()
                        .get(new TypeReference<List<DocumentCondition>>() {
                        })
                        .forEach(c -> feedQuery(ravenQuery.orElse(), c, ids));
                return;
            default:
                throw new UnsupportedOperationException("The condition " + condition.condition()
                        + " is not supported from ravendb diana driver");
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

        public Optional<IDocumentQuery<HashMap>> getRavenQuery() {
            return Optional.ofNullable(ravenQuery);
        }
    }
}

