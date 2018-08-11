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
package org.jnosql.diana.cassandra.column;

import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.column.ColumnCondition;
import org.jnosql.diana.api.column.ColumnQuery;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * A CouchDB specialization of {@link ColumnQuery} that allows query with bookmark which can do pagination.
 *
 * @see CassandraQuery#of(ColumnQuery)
 * @see CassandraQuery#of(ColumnQuery, String)
 */
public final class CassandraQuery implements ColumnQuery {

    private final ColumnQuery query;

    private String bookmark;


    private CassandraQuery(ColumnQuery query) {
        this.query = query;
    }


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
    public String getColumnFamily() {
        return query.getColumnFamily();
    }

    @Override
    public Optional<ColumnCondition> getCondition() {
        return query.getCondition();
    }

    @Override
    public List<Sort> getSorts() {
        return query.getSorts();
    }

    @Override
    public List<String> getColumns() {
        return query.getColumns();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CassandraQuery that = (CassandraQuery) o;
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
     * returns a new instance of {@link CassandraQuery}
     *
     * @param query the {@link ColumnQuery}
     * @return a new instance
     * @throws NullPointerException when query is null
     */
    public static CassandraQuery of(ColumnQuery query) {
        Objects.requireNonNull(query, "query is required ");
        return new CassandraQuery(query);
    }

    /**
     * returns a new instance of {@link CassandraQuery}
     *
     * @param query    the {@link ColumnQuery}
     * @param bookmark {@link CassandraQuery#bookmark}
     * @return a new instance
     * @throws NullPointerException when there is null parameter
     */
    public static CassandraQuery of(ColumnQuery query, String bookmark) {
        Objects.requireNonNull(query, "query is required ");
        Objects.requireNonNull(bookmark, "bookmark is required ");
        CassandraQuery couchDBDocumentQuery = new CassandraQuery(query);
        couchDBDocumentQuery.bookmark = bookmark;
        return couchDBDocumentQuery;
    }

}
