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
package org.eclipse.jnosql.diana.cassandra.column;


import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.lang.Boolean.FALSE;

class CassandraProperties {

    private static final int DEFAULT_PORT = 9042;

    private static final String DEFAULT_DATA_CENTER = "datacenter1";

    private List<String> queries = new ArrayList<>();

    private List<String> nodes = new ArrayList<>();

    private Optional<String> name;

    private int port;

    private String dataCenter;

    public void addQuery(String query) {
        this.queries.add(query);
    }

    public void addNodes(String node) {
        this.nodes.add(node);
    }

    public List<String> getQueries() {
        return queries;
    }

    public CqlSessionBuilder createCluster() {
        CqlSessionBuilder builder = CqlSession.builder();
        nodes.stream().map(h -> new InetSocketAddress(h, port)).forEach(builder::addContactPoint);
        name.ifPresent(builder::withApplicationName);
        builder.withLocalDatacenter(dataCenter);
        return builder;
    }

    public ExecutorService createExecutorService() {
        return Executors.newCachedThreadPool();
    }

    public static CassandraProperties of(Map<String, String> configurations) {
        SettingsBuilder builder = Settings.builder();
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        Settings settings = builder.build();

        CassandraProperties cp = new CassandraProperties();
        settings.prefix(Arrays.asList(OldCassandraConfigurations.HOST.get(),
                CassandraConfigurations.HOST.get(),
                Configurations.HOST.get())).stream()
                .map(Object::toString).forEach(cp::addNodes);

        settings.prefix(Arrays.asList(OldCassandraConfigurations.QUERY.get(), CassandraConfigurations.QUERY.get()))
                .stream().map(Object::toString).forEach(cp::addQuery);

        cp.port = settings.get(Arrays.asList(OldCassandraConfigurations.PORT.get(), CassandraConfigurations.PORT.get()))
                .map(Object::toString).map(Integer::parseInt).orElse(DEFAULT_PORT);

        cp.name = settings.get(Arrays.asList(OldCassandraConfigurations.NAME.get(), CassandraConfigurations.NAME.get()))
                .map(Object::toString);
        cp.dataCenter = settings.get(CassandraConfigurations.DATA_CENTER.get()).map(Object::toString)
                .orElse(DEFAULT_DATA_CENTER);
        return cp;
    }
}
