/*
 *  Copyright (c) 2022 Otávio Santana and others
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
package org.eclipse.jnosql.communication.couchbase.document;

import com.couchbase.client.java.json.JsonObject;

import java.util.Objects;

final class N1QLQuery {

    private final String query;

    private final JsonObject params;

    N1QLQuery(String query, JsonObject params) {
        this.query = query;
        this.params = params;
    }

    public String getQuery() {
        return query;
    }

    public JsonObject getParams() {
        return params;
    }

    public boolean isEmpty() {
        return this.params.isEmpty();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        N1QLQuery n1QLQuery = (N1QLQuery) o;
        return Objects.equals(query, n1QLQuery.query) && Objects.equals(params, n1QLQuery.params);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, params);
    }

    @Override
    public String toString() {
        return "N1QLQuery{" +
                "query='" + query + '\'' +
                ", params=" + params +
                '}';
    }

    static N1QLQuery of(StringBuilder query, JsonObject params) {
        return new N1QLQuery(query.toString(), params);
    }
}
