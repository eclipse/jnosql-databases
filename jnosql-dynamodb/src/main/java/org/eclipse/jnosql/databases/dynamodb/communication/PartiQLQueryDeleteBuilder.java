/*
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 * The Eclipse Public License is available at http://www.eclipse.org/legal/epl-v10.html
 * and the Apache License v2.0 is available at http://www.opensource.org/licenses/apache2.0.php.
 *
 * You may elect to redistribute this code under either of these licenses.
 *
 * Contributors:
 *
 * Maximillian Arruda
 */

package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedList;
import java.util.List;

public class PartiQLQueryDeleteBuilder extends PartiQLQueryBuilder {

    private final String table;

    private final String partitionKey;

    private final DocumentDeleteQuery documentDeleteQuery;

    public PartiQLQueryDeleteBuilder(String table, String partitionKey, DocumentDeleteQuery documentDeleteQuery) {
        this.table = table;
        this.partitionKey = partitionKey;
        this.documentDeleteQuery = documentDeleteQuery;
    }

    @Override
    public PartiQLQuery get() {

        var query = new StringBuilder();
        List<AttributeValue> params = new LinkedList<>();

        query.append("DELETE FROM ").append(table).append(" WHERE ");
        condition(DocumentCondition.eq(Document.of(partitionKey, documentDeleteQuery.name())), query, params);
        this.documentDeleteQuery.condition().ifPresent(c -> {
            query.append(" AND ");
            condition(c, query, params);
        });

        return new PartiQLQuery(query.toString(), params, null, null);
    }
}
