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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Boolean.FALSE;

class CassandraProperties {

    private static final String DEFAULT_THREADS_NUMBER = Integer.toString(Runtime.getRuntime().availableProcessors());
    private static final String CASSANDRA_HOSTER = "cassandra-host";
    private static final String CASSANDRA_PORT = "cassandra-port";
    private static final String CASSANDRA_QUERY = "cassandra-query";
    private static final String CASSANDRA_THREADS_NUMBER = "cassandra-threads-number";
    private static final String CASSANDRA_SSL = "cassandra-ssl";
    private static final String CASSANDRA_METRICS = "cassandra-metrics";
    private static final String CASSANDRA_JMX = "cassandra-jmx";

    private List<String> queries = new ArrayList<>();


    private String numTreads;

    private List<String> nodes = new ArrayList<>();

    private Optional<String> name = Optional.empty();

    private OptionalInt maxSchemaAgreementWaitSeconds = OptionalInt.empty();

    private OptionalInt port = OptionalInt.empty();

    private boolean withoutJXMReporting;

    private boolean withoutMetrics;

    private boolean withSSL;


    public void addQuery(String query) {
        this.queries.add(query);
    }

    public void addNodes(String node) {
        this.nodes.add(node);
    }

    public List<String> getQueries() {
        return queries;
    }

    public Cluster createCluster() {
        Cluster.Builder builder = Cluster.builder();

        nodes.forEach(builder::addContactPoint);
        name.ifPresent(n -> builder.withClusterName(n));
        maxSchemaAgreementWaitSeconds.ifPresent(m -> builder.withMaxSchemaAgreementWaitSeconds(m));
        port.ifPresent(p -> builder.withPort(p));

        if (withoutJXMReporting) {
            builder.withoutJMXReporting();
        }
        if (withoutMetrics) {
            builder.withoutMetrics();
        }
        if (withSSL) {
            builder.withSSL();
        }
        return builder.build();
    }

    public ExecutorService createExecutorService() {
        return Executors.newFixedThreadPool(Integer.valueOf(numTreads));
    }

    public static CassandraProperties of(Map<String, String> configurations) {
        CassandraProperties cp = new CassandraProperties();

        configurations.keySet().stream().filter(s -> s.startsWith(CASSANDRA_HOSTER))
                .map(configurations::get).forEach(cp::addNodes);

        configurations.keySet().stream().filter(s -> s.startsWith(CASSANDRA_QUERY))
                .sorted().map(configurations::get).forEach(cp::addQuery);

        cp.port = OptionalInt.of(Integer.valueOf(configurations.getOrDefault(CASSANDRA_PORT, "9042")));
        cp.numTreads = configurations.getOrDefault(CASSANDRA_THREADS_NUMBER, DEFAULT_THREADS_NUMBER);
        cp.withSSL = Boolean.valueOf(configurations.getOrDefault(CASSANDRA_SSL, FALSE.toString()));
        cp.withoutMetrics = Boolean.valueOf(configurations.getOrDefault(CASSANDRA_METRICS, FALSE.toString()));
        cp.withoutJXMReporting = Boolean.valueOf(configurations.getOrDefault(CASSANDRA_JMX, FALSE.toString()));
        return cp;
    }
}
