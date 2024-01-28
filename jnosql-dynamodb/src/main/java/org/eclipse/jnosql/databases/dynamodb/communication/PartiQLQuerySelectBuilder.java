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

import jakarta.data.Direction;
import org.eclipse.jnosql.communication.document.Document;
import org.eclipse.jnosql.communication.document.DocumentCondition;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class PartiQLQuerySelectBuilder extends PartiQLQueryBuilder {
    private final String table;
    private final String partitionKey;
    private final DocumentQuery documentQuery;

    public PartiQLQuerySelectBuilder(String table, String partitionKey, DocumentQuery documentQuery) {
        this.table = table;
        this.partitionKey = partitionKey;
        this.documentQuery = documentQuery;
    }


    @Override
    public PartiQLQuery get() {
        var query = new StringBuilder();
        List<AttributeValue> params = new LinkedList<>();

        query.append("SELECT ");
        query.append(select()).append(' ');
        query.append("FROM ").append(table).append(' ');
        query.append("WHERE ");
        condition(DocumentCondition.eq(Document.of(partitionKey, documentQuery.name())), query, params);
        this.documentQuery.condition().ifPresent(c -> {
            query.append(" AND ");
            condition(c, query, params);
        });

        if (!this.documentQuery.sorts().isEmpty()) {
            query.append(" ORDER BY ");
            var order = this.documentQuery.sorts().stream()
                    .map(s -> s.property() + " " + (s.isAscending() ? Direction.ASC : Direction.DESC))
                    .collect(Collectors.joining(", "));
            query.append(order);
        }

        return new PartiQLQuery(query.toString(), params, this.documentQuery.limit(), this.documentQuery.skip());
    }

    String select() {
        var documents = documentQuery.documents();
        if (documents.isEmpty()){
            return "*";
        }
        return partitionKey + "," + String.join(", ", documents);
    }
}
