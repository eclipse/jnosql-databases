/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentQuery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A CouchDB specialization of {@link DocumentQuery} that allows query with bookmark which can do pagination.
 *
 * @see CouchDBDocumentQuery#of(DocumentQuery)
 * @see CouchDBDocumentQuery#of(DocumentQuery, String)
 */
public final class CouchDBDocumentQuery implements DocumentQuery {


    private final DocumentQuery query;

    private String bookmark;


    private CouchDBDocumentQuery(DocumentQuery query) {
        this.query = query;
    }

    /**
     * The A string that enables you to specify which page of results you require. Used for paging
     * through result sets. Every query returns an opaque string under the bookmark key that can
     * then be passed back in a query to get the next page of results. If any part of the selector query
     * changes between requests, the results are undefined. Optional, default: null
     *
     * @return the bookmark
     */
    public Optional<String> getBookmark() {
        return Optional.ofNullable(bookmark);
    }


    @Override
    public long getLimit() {
        return query.getLimit();
    }

    @Override
    public long getSkip() {
        return query.getSkip();
    }

    @Override
    public String getDocumentCollection() {
        return query.getDocumentCollection();
    }

    @Override
    public Optional<DocumentCondition> getCondition() {
        return query.getCondition();
    }

    @Override
    public List<Sort> getSorts() {
        return query.getSorts();
    }

    @Override
    public List<String> getDocuments() {
        return query.getDocuments();
    }

    /**
     * returns a new instance of {@link CouchDBDocumentQuery}
     *
     * @param query the {@link DocumentQuery}
     * @return a new instance
     * @throws NullPointerException when query is null
     */
    public static CouchDBDocumentQuery of(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required ");
        return new CouchDBDocumentQuery(query);
    }

    /**
     * returns a new instance of {@link CouchDBDocumentQuery}
     *
     * @param query    the {@link DocumentQuery}
     * @param bookmark {@link CouchDBDocumentQuery#bookmark}
     * @return a new instance
     * @throws NullPointerException when there is null parameter
     */
    public static CouchDBDocumentQuery of(DocumentQuery query, String bookmark) {
        Objects.requireNonNull(query, "query is required ");
        Objects.requireNonNull(bookmark, "bookmark is required ");
        CouchDBDocumentQuery couchDBDocumentQuery = new CouchDBDocumentQuery(query);
        couchDBDocumentQuery.bookmark = bookmark;
        return couchDBDocumentQuery;
    }


}
