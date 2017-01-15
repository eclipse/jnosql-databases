/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.jnosql.diana.cassandra.column;


import com.datastax.driver.core.Cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.jnosql.diana.api.column.ColumnConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

public class CassandraConfiguration implements ColumnConfiguration<CassandraDocumentEntityManagerFactory> {

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
    public CassandraDocumentEntityManagerFactory getManagerFactory() {
        Map<String, String> configuration = ConfigurationReader.from(CASSANDRA_FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }
}
