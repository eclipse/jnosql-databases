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
package org.eclipse.jnosql.communication.dynamodb;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.dynamodb.keyvalue.DynamoDBKeyValueConfiguration;
import org.testcontainers.containers.GenericContainer;

public class DynamoDBTestUtils {

    private static GenericContainer dynamodb =
            new GenericContainer("amazon/dynamodb-local:latest")
                    .withExposedPorts(8000)
                    .withEnv("AWS_ACCESS_KEY_ID", "aws --profile default configure get aws_access_key_id")
                    .withEnv("AWS_SECRET_ACCESS_KEY", "aws --profile default configure get aws_secret_access_key");
    //.withCommand("--rm");

    // .waitingFor(Wait.forHttp("/")
    //       .forStatusCode(200));


    public static BucketManagerFactory get() {
        dynamodb.start();
        DynamoDBKeyValueConfiguration configuration = new DynamoDBKeyValueConfiguration();
        String endpoint = "http://" + dynamodb.getHost() + ":" + dynamodb.getFirstMappedPort();
        return configuration.apply(Settings.builder()
                .put(DynamoDBConfigurations.ENDPOINT, endpoint).build());
    }

    public static void shutDown() {
        dynamodb.close();
    }
}
