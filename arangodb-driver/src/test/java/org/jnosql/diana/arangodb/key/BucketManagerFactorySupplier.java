/*
 *  Copyright (c) 2017 Otávio Santana and others
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
package org.jnosql.diana.arangodb.key;


import org.jnosql.diana.api.key.BucketManagerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.function.Supplier;

enum BucketManagerFactorySupplier implements Supplier<BucketManagerFactory> {

    INSTANCE;

    private final GenericContainer arangodb =
            new GenericContainer("arangodb/arangodb:latest")
                    .withExposedPorts(8529)
                    .withEnv("ARANGO_NO_AUTH", "1")
                    .waitingFor(Wait.forHttp("/")
                            .forStatusCode(200));


    @Override
    public BucketManagerFactory get() {
        arangodb.start();
        ArangoDBKeyValueConfiguration configuration = new ArangoDBKeyValueConfiguration();
        configuration.addHost(arangodb.getContainerIpAddress(), arangodb.getFirstMappedPort());
        return configuration.get();
    }
}
