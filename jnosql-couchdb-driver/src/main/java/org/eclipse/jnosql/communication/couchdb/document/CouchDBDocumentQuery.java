/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document;

import jakarta.data.repository.Sort;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;

import java.util.List;
import java.util.Map;
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

    void setBookmark(Map<String, Object> json) {
        json.computeIfPresent(CouchDBConstant.BOOKMARK, (k, v) -> this.bookmark = v.toString());
    }

    @Override
    public long getLimit() {
        return query.limit();
    }

    @Override
    public long getSkip() {
        return query.skip();
    }

    @Override
    public String getDocumentCollection() {
        return query.name();
    }

    @Override
    public Optional<DocumentCondition> getCondition() {
        return query.condition();
    }

    @Override
    public List<Sort> getSorts() {
        return query.sorts();
    }

    @Override
    public List<String> getDocuments() {
        return query.getDocuments();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CouchDBDocumentQuery that = (CouchDBDocumentQuery) o;
        return Objects.equals(query, that.query) &&
                Objects.equals(bookmark, that.bookmark);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, bookmark);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchDBDocumentQuery{");
        sb.append("query=").append(query);
        sb.append(", bookmark='").append(bookmark).append('\'');
        sb.append('}');
        return sb.toString();
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
