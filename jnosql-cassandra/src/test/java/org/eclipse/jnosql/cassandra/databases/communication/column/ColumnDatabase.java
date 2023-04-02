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

package org.eclipse.jnosql.cassandra.databases.communication.column;

import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;
import org.testcontainers.containers.CassandraContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

enum ColumnDatabase implements Supplier<CassandraColumnManagerFactory> {

    INSTANCE;

    private final CassandraContainer cassandra =
            (CassandraContainer) new CassandraContainer("cassandra:latest")
                    .withExposedPorts(9042);

    {
        cassandra.start();
    }

    @Override
    public CassandraColumnManagerFactory get() {
        Settings settings = getSettings();
        CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
        return cassandraConfiguration.apply(settings);
    }

    Settings getSettings() {
        Map<String, Object> configuration = new HashMap<>(ConfigurationReader.from("cassandra.properties"));
        configuration.put(CassandraConfigurations.HOST.get()+".1", cassandra.getHost());
        configuration.put(CassandraConfigurations.PORT.get(), cassandra.getFirstMappedPort());
        return Settings.of(configuration);
    }
}
