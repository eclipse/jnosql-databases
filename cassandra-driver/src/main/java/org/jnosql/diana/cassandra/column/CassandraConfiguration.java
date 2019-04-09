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

package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.Cluster;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.column.UnaryColumnConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static java.util.Objects.requireNonNull;

/**
 * The Cassandra implementation to {@link UnaryColumnConfiguration} that returns
 * {@link CassandraColumnFamilyManagerFactory}
 * This configuration reads "diana-cassandra.properties" files and has the following configuration:
 * <p>cassandra-host-: The Cassandra host as prefix, you can set how much you want just setting the number order,
 * eg: cassandra-host-1 = host, cassandra-host-2 = host2</p>
 * <p>cassandra-query-: The Cassandra query to run when an instance is started, you can set how much you want just
 * setting the order number, eg: cassandra-query-1=cql, cassandra-query-2=cql2... </p>
 * <p>cassandra-threads-number: The number of executor to run on Async process, if it isn't defined that will use the number of processor</p>
 * <p>cassandra-ssl: Define ssl, the default value is false</p>
 * <p>cassandra-metrics: enable metrics, the default value is true</p>
 * <p>cassandra-jmx: enable JMX, the default value is true</p>
 */
public class CassandraConfiguration implements UnaryColumnConfiguration<CassandraColumnFamilyManagerFactory> {

    static final String CASSANDRA_FILE_CONFIGURATION = "diana-cassandra.properties";

    public CassandraColumnFamilyManagerFactory getManagerFactory(Map<String, String> configurations) {
        requireNonNull(configurations);
        CassandraProperties properties = CassandraProperties.of(configurations);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraColumnFamilyManagerFactory(properties.createCluster(), properties.getQueries(), executorService);
    }

    public CassandraColumnFamilyManagerFactory getEntityManagerFactory(Cluster cluster) {
        requireNonNull(cluster, "Cluster is required");

        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        CassandraProperties properties = CassandraProperties.of(configuration);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraColumnFamilyManagerFactory(cluster, properties.getQueries(), executorService);
    }

    @Override
    public CassandraColumnFamilyManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }

    @Override
    public CassandraColumnFamilyManagerFactory get(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");
        Map<String, String> configurations = new HashMap<>();
        settings.forEach((key, value) -> configurations.put(key, value.toString()));
        return getManagerFactory(configurations);
    }

    @Override
    public CassandraColumnFamilyManagerFactory getAsync() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }

    @Override
    public CassandraColumnFamilyManagerFactory getAsync(Settings settings) throws NullPointerException {
        return get(settings);
    }
}
