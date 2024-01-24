/*
 *  Copyright (c) 2023 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.databases.oracle.communication;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.document.DocumentManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.BucketManagerFactory;
import org.eclipse.jnosql.communication.keyvalue.KeyValueConfiguration;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.function.Supplier;

public enum Database implements Supplier<BucketManagerFactory> {

    INSTANCE;
    private final GenericContainer<?> container = new GenericContainer<>
            (DockerImageName.parse("ghcr.io/oracle/nosql:latest-ce"))
            .withExposedPorts(8080);

    {
        container.start();
    }


    @Override
    public BucketManagerFactory get() {
        KeyValueConfiguration configuration = new OracleNoSQLKeyValueConfiguration();
        Settings settings = Settings.builder()
                .put(OracleNoSQLConfigurations.HOST, host())
                .build();
        return configuration.apply(settings);
    }

    public String host() {
        return "http://" + container.getHost() + ":" + container.getFirstMappedPort();
    }

    public DocumentManagerFactory managerFactory() {
        DocumentConfiguration configuration = DocumentConfiguration.getConfiguration();
        Settings settings = Settings.builder()
                .put(OracleNoSQLConfigurations.HOST, host())
                .build();
        return configuration.apply(settings);
    }
}
