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
package org.eclipse.jnosql.communication.couchbase.document;

import com.couchbase.client.java.json.JsonObject;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class N1QLQuery {

    private final String query;

    private final JsonObject params;

    private final List<String> ids;

    N1QLQuery(String query, JsonObject params, List<String> ids) {
        this.query = query;
        this.params = params;
        this.ids = ids;
    }

    public String getQuery() {
        return query;
    }

    public JsonObject getParams() {
        return params;
    }

    public List<String> getIds() {
        return Collections.unmodifiableList(ids);
    }

    public boolean hasParameter() {
        return this.params.isEmpty();
    }

    public boolean hasOnlyIds() {
        return hasIds() && hasParameter();
    }

    public boolean hasIds() {
        return !this.ids.isEmpty();
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
        return Objects.equals(query, n1QLQuery.query) && Objects.equals(params, n1QLQuery.params)
                && Objects.equals(ids, n1QLQuery.ids);
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, params, ids);
    }

    @Override
    public String toString() {
        return "N1QLQuery{" +
                "query='" + query + '\'' +
                ", params=" + params +
                ", ids=" + ids +
                '}';
    }

    static N1QLQuery of(StringBuilder query, JsonObject params, List<String> ids) {
        return new N1QLQuery(query.toString(), params, ids);
    }
}
