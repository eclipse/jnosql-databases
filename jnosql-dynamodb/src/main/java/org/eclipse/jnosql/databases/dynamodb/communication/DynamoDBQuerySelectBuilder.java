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
import org.eclipse.jnosql.communication.document.DocumentQuery;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.HashMap;

class DynamoDBQuerySelectBuilder extends DynamoDBQueryBuilder {

    private final String table;

    private final String partitionKey;

    private final DocumentQuery documentQuery;

    public DynamoDBQuerySelectBuilder(String table,
                                      String partitionKey,
                                      DocumentQuery documentQuery) {
        this.table = table;
        this.partitionKey = partitionKey;
        this.documentQuery = documentQuery;
    }

    @Override
    public DynamoDBQuery get() {

        var filterExpression = new StringBuilder();
        var expressionAttributeNames = new HashMap<String, String>();
        var expressionAttributeValues = new HashMap<String, AttributeValue>();

        super.condition(
                DocumentCondition.eq(Document.of(partitionKey, documentQuery.name())),
                filterExpression, expressionAttributeNames, expressionAttributeValues);

        this.documentQuery.condition().ifPresent(c -> {
            filterExpression.append(" AND ");
            super.condition(c,
                    filterExpression,
                    expressionAttributeNames,
                    expressionAttributeValues);
        });


        return new DynamoDBQuery(
                table,
                projectionExpression(),
                filterExpression.toString(),
                expressionAttributeNames,
                expressionAttributeValues);
    }

    String projectionExpression() {
        var documents = documentQuery.documents();
        if (documents.isEmpty()) {
            return null;
        }
        return String.join(", ", documents);
    }


}
