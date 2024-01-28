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

import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.stream.Stream;

/**
 * A document manager interface for DynamoDB database operations.
 */
public interface DynamoDBDocumentManager extends DocumentManager {

    /**
     * DynamoDB supports a limited subset of
     * <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html">PartiQL</a>.
     * This method executes a PartiQL query and returns a stream of DocumentEntity objects.
     *
     * @param query the PartiQL query
     * @return a {@link Stream} of {@link DocumentEntity} representing the query result
     * @throws NullPointerException  when the query is null
     */
    Stream<DocumentEntity> partiQL(String query);

    /**
     * DynamoDB supports a limited subset of <a href="https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ql-reference.html">PartiQL</a>.
     * This method executes a PartiQL query with parameters and returns a stream of DocumentEntity objects.
     * <p>Example query: {@code SELECT * FROM users WHERE status = ?}</p>
     *
     * @param query the PartiQL query
     * @return a {@link Stream} of {@link DocumentEntity} representing the query result
     * @throws NullPointerException  when the query is null
     */
    Stream<DocumentEntity> partiQL(String query, Object... params);


    /**
     * @return a {@link DynamoDbClient} instance for custom utilization
     */
    DynamoDbClient dynamoDbClient();


}
