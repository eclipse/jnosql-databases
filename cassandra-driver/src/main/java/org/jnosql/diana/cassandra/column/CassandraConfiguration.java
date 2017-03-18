/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.Cluster;
import org.jnosql.diana.api.column.UnaryColumnConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * The Cassandra implementation to {@link UnaryColumnConfiguration} that returns
 * {@link CassandraDocumentEntityManagerFactory}
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
public class CassandraConfiguration implements UnaryColumnConfiguration<CassandraDocumentEntityManagerFactory> {

    private static final String CASSANDRA_FILE_CONFIGURATION = "diana-cassandra.properties";

    public CassandraDocumentEntityManagerFactory getManagerFactory(Map<String, String> configurations) {
        Objects.requireNonNull(configurations);
        CassandraProperties properties = CassandraProperties.of(configurations);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraDocumentEntityManagerFactory(properties.createCluster(), properties.getQueries(), executorService);
    }

    public CassandraDocumentEntityManagerFactory getEntityManagerFactory(Cluster cluster) {
        Objects.requireNonNull(cluster, "Cluster is required");

        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        CassandraProperties properties = CassandraProperties.of(configuration);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraDocumentEntityManagerFactory(cluster, properties.getQueries(), executorService);
    }

    @Override
    public CassandraDocumentEntityManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }

    @Override
    public CassandraDocumentEntityManagerFactory getAsync() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }
}
