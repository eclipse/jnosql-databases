/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.jnosql.diana.orientdb.document;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.util.function.Supplier;

public enum  ManagerFactorySupplier implements Supplier<OrientDBDocumentCollectionManagerFactory> {

    INSTANCE;

    private final GenericContainer orientDB =
            new GenericContainer("orientdb:latest")
                    .withExposedPorts(2424)
                    .withExposedPorts(2480)
                    .withEnv("ORIENTDB_ROOT_PASSWORD", "rootpwd")
                    .waitingFor(Wait.defaultWaitStrategy());

    {
        orientDB.start();
    }

    @Override
    public OrientDBDocumentCollectionManagerFactory get() {
        OrientDBDocumentConfiguration configuration = new OrientDBDocumentConfiguration();
        configuration.setHost(orientDB.getContainerIpAddress() + ':' + orientDB.getFirstMappedPort(););
        configuration.setUser("root");
        configuration.setPassword("rootpwd");
        return configuration.get();
    }
}
