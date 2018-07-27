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
package org.jnosql.diana.couchbase.configuration;

import org.testcontainers.containers.FixedHostPortGenericContainer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;

public class CouchbaseContainer {

    private static CouchbaseContainer instance;
    private final GenericContainer couchbase;

    private CouchbaseContainer(String user, String password) {
        this.couchbase =
                new FixedHostPortGenericContainer("couchbase:latest")
                        .withFixedExposedPort(8091, 8091)
                        .withFixedExposedPort(8092, 8092)
                        .withFixedExposedPort(8093, 8093)
                        .withFixedExposedPort(8094, 8094)
                        .withFixedExposedPort(11210, 11210)
                        .waitingFor(getCompositeWaitStrategy());

        couchbase.start();
        new CouchbaseContainerConfiguration(couchbase, user, password).configure();
    }

    public static CouchbaseContainer start(String user, String password) {
        if (instance == null) {
            instance = new CouchbaseContainer(user, password);
        }
        return instance;
    }


    private WaitStrategy getCompositeWaitStrategy() {
        return new WaitAllStrategy()
                .withStrategy(new HttpWaitStrategy()
                        .forPort(8091)
                        .forPath("/ui/index.html")
                        .forStatusCode(200));
    }

    public GenericContainer getContainer() {
        return couchbase;
    }

}
