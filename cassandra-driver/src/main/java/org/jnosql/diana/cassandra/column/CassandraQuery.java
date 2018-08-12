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

import com.datastax.driver.core.PagingState;
import org.jnosql.diana.api.Sort;
import org.jnosql.diana.api.column.ColumnCondition;
import org.jnosql.diana.api.column.ColumnQuery;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * A Cassandra specialization of {@link ColumnQuery} that allows query with paging state which can do pagination.
 *
 * @see CassandraQuery#of(ColumnQuery)
 * @see CassandraQuery#of(ColumnQuery, String)
 */
public final class CassandraQuery implements ColumnQuery {

    private static final String EXHAUSTED = "EXHAUSTED";
    private static final Predicate<String> EQUALS = EXHAUSTED::equals;
    private static final Predicate<String> NOT_EQUALS = EQUALS.negate();

    private final ColumnQuery query;

    private String pagingState;


    private CassandraQuery(ColumnQuery query) {
        this.query = query;
    }


    public Optional<String> getPagingState() {
        return Optional.ofNullable(pagingState);
    }

    Optional<PagingState> toPatingState() {
        return getPagingState().filter(NOT_EQUALS).map(PagingState::fromString);
    }


    void setPagingState(PagingState pagingState) {
        if (pagingState != null) {
            this.pagingState = pagingState.toString();
        }
    }


    void setExhausted(boolean exhausted) {
        if (exhausted) {
            this.pagingState = EXHAUSTED;
        }
    }


    boolean isExhausted() {
        return EXHAUSTED.equals(pagingState);
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
                Objects.equals(pagingState, that.pagingState);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, pagingState);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchDBDocumentQuery{");
        sb.append("query=").append(query);
        sb.append(", pagingState='").append(pagingState).append('\'');
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
     * @param query       the {@link ColumnQuery}
     * @param pagingState {@link CassandraQuery#pagingState}
     * @return a new instance
     * @throws NullPointerException when there is null parameter
     */
    public static CassandraQuery of(ColumnQuery query, String pagingState) {
        Objects.requireNonNull(query, "query is required ");
        Objects.requireNonNull(pagingState, "pagingState is required ");
        CassandraQuery cassandraQuery = new CassandraQuery(query);
        cassandraQuery.pagingState = pagingState;
        return cassandraQuery;
    }
}
