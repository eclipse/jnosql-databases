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
package org.eclipse.jnosql.databases.dynamodb.communication;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

enum DynamoDBTestUtils {

    INSTANCE;

    private final GenericContainer dynamodb =
            new GenericContainer("amazon/dynamodb-local:latest")
                    .withExposedPorts(8000)
                    .waitingFor(Wait.defaultWaitStrategy());

    BucketManagerFactory getBucketManagerFactory() {
        Settings settings = getSettings();
        DynamoDBKeyValueConfiguration configuration = new DynamoDBKeyValueConfiguration();
        return configuration.apply(settings);
    }

    DocumentManagerFactory getDocumentManagerFactory() {
        Settings settings = getSettings();
        var configuration = new DynamoDBDocumentConfiguration();
        return configuration.apply(settings);
    }

    Settings getSettings() {
        dynamodb.start();
        return Settings.builder()
                .put(DynamoDBConfigurations.ENDPOINT, "http://" + dynamodb.getHost() + ":" + dynamodb.getFirstMappedPort())
                .put(DynamoDBConfigurations.AWS_ACCESSKEY, System.getProperty("AWS_ACCESS_KEY_ID","DUMMYIDEXAMPLE"))
                .put(DynamoDBConfigurations.AWS_SECRET_ACCESS, System.getProperty("AWS_SECRET_ACCESS_KEY","DUMMYEXAMPLEKEY"))
                .put(DynamoDBConfigurations.REGION, "us-west-2").build();
    }

    void shutDown() {
        dynamodb.close();
    }
}
