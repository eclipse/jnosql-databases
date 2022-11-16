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

package org.eclipse.jnosql.communication.cassandra.column;


import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import jakarta.nosql.Settings;
import jakarta.nosql.column.ColumnConfiguration;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * The Cassandra implementation to {@link ColumnConfiguration}  that returns
 * {@link CassandraColumnManagerFactory}
 * This configuration reads "diana-cassandra.properties" files and has the following configuration:
 * <p>cassandra.host-: The Cassandra host as prefix, you can set how much you want just setting the number order,
 * eg: cassandra.host-1 = host, cassandra.host-2 = host2</p>
 * <p>cassandra.query.: The Cassandra query to run when an instance is started, you can set how much you want just
 * setting the order number, eg: cassandra.query.1=cql, cassandra.query.2=cql2... </p>
 * <p>cassandra.threads.number: The number of executor to run on Async process, if it isn't defined that will use the number of processor</p>
 * <p>cassandra.ssl: Define ssl, the default value is false</p>
 * <p>cassandra.metrics: enable metrics, the default value is true</p>
 * <p>cassandra.jmx: enable JMX, the default value is true</p>
 *
 * @see CassandraConfigurations
 */
public final class CassandraConfiguration implements ColumnConfiguration {


    private CassandraColumnManagerFactory getManagerFactory(Map<String, String> configurations) {
        requireNonNull(configurations);
        CassandraProperties properties = CassandraProperties.of(configurations);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraColumnManagerFactory(properties.createCluster(), properties.getQueries(), executorService);
    }


    @Override
    public CassandraColumnManagerFactory apply(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");
        Map<String, String> configurations = new HashMap<>();
        for (String key : settings.keySet()) {
            configurations.put(key, settings.get(key, String.class).orElseThrow());
        }
        return getManagerFactory(configurations);
    }

}
