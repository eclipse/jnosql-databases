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
package org.eclipse.jnosql.databases.cassandra.communication;

import jakarta.data.Sort;
import org.eclipse.jnosql.communication.column.ColumnCondition;
import org.eclipse.jnosql.communication.column.ColumnQuery;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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

    /**
     * This object represents the next page to be fetched if the query is multi page.
     * It can be saved and reused later on the same statement.
     */
    private String pagingState;


    private CassandraQuery(ColumnQuery query) {
        this.query = query;
    }


    /**
     * {@link CassandraQuery#pagingState}
     *
     * @return the {@link CassandraQuery#pagingState}
     */
    public Optional<String> getPagingState() {
        return Optional.ofNullable(pagingState);
    }

    Optional<ByteBuffer> toPaginate() {
        return getPagingState().filter(NOT_EQUALS).map(s -> ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8)));
    }

    void setPagingState(ByteBuffer pagingState) {
        if (pagingState != null) {
            this.pagingState = StandardCharsets.UTF_8.decode(pagingState).toString();
        }
    }


    void setExhausted(boolean exhausted) {
        synchronized (this) {
            if (exhausted) {
                this.pagingState = EXHAUSTED;
            }
        }
    }


    boolean isExhausted() {
        return EXHAUSTED.equals(pagingState);
    }

    @Override
    public long limit() {
        return query.limit();
    }

    @Override
    public long skip() {
        return query.skip();
    }

    @Override
    public String name() {
        return query.name();
    }

    @Override
    public Optional<ColumnCondition> condition() {
        return query.condition();
    }

    @Override
    public List<Sort> sorts() {
        return query.sorts();
    }

    @Override
    public List<String> columns() {
        return query.columns();
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