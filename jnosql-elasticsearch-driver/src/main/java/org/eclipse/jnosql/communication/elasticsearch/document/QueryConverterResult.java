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
package org.eclipse.jnosql.communication.elasticsearch.document;

import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;

class QueryConverterResult {


    private final QueryBuilder statement;

    private final List<String> ids;

    QueryConverterResult(QueryBuilder statement, List<String> ids) {
        this.statement = statement;
        this.ids = ids;
    }


    QueryBuilder getStatement() {
        return statement;
    }

    List<String> getIds() {
        return ids;
    }

    public boolean hasId() {
        return !ids.isEmpty();
    }

    public boolean hasStatement() {
        return statement != null || ids.isEmpty();
    }

    public boolean hasQuery() {
        return statement != null;
    }
}
