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
 *   Alessandro Moscatelli
 */

package org.eclipse.jnosql.communication.dynamodb.document;

import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.eclipse.jnosql.communication.dynamodb.DynamoTableUtils;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory {

    private final DynamoDbClient client;
    
    DynamoDBDocumentCollectionManagerFactory(DynamoDbClient client) {
        this.client = client;
    }
    
    @Override
    public DynamoDBDocumentCollectionManager get(String tableName) {
        return new DynamoDBDocumentCollectionManager(client, tableName, !DynamoTableUtils.existTable(tableName, client));
    }

    @Override
    public void close() {
        client.close();
    }
    
}
