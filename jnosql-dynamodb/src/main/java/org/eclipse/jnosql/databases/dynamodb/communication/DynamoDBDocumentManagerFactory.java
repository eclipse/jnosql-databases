/*
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
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

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.Optional;

public class DynamoDBDocumentManagerFactory implements DocumentManagerFactory {

    private final DynamoDbClient dynamoDB;
    private final Settings settings;

    public DynamoDBDocumentManagerFactory(DynamoDbClient dynamoDB, Settings settings) {
        this.dynamoDB = dynamoDB;
        this.settings = settings;
    }

    @Override
    public DynamoDBDocumentManager apply(String database) {
        return new DefaultDynamoDBDocumentManager(database, dynamoDB, settings);
    }

    @Override
    public void close() {
        Optional.ofNullable(this.dynamoDB).ifPresent(DynamoDbClient::close);
    }
}
