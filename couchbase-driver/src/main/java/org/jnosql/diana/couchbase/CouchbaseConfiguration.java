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
package org.jnosql.diana.couchbase;


import org.jnosql.diana.api.Configurations;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

/**
 * The configuration base to all configuration implementation on couchbase
 */
public abstract class CouchbaseConfiguration {

    private static final String FILE_CONFIGURATION = "diana-couchbase.properties";

    protected final List<String> nodes = new ArrayList<>();

    protected String user;

    protected String password;

    public CouchbaseConfiguration() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        SettingsBuilder builder = Settings.builder();
        configuration.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        Settings settings = builder.build();

        update(settings);
    }

    protected void update(Settings settings) {
        getHosts(settings).forEach(this::add);
        this.user = getUser(settings);
        this.password = getUser(settings);
    }

    protected String getUser(Settings settings) {
        return settings.get(asList(Configurations.USER.get(),
                CouchbaseConfigurations.USER.get(),
                OldCouchbaseConfigurations.USER.get()))
                .map(Object::toString).orElse(null);
    }

    protected String getPassword(Settings settings) {

        return settings.get(asList(Configurations.PASSWORD.get(),
                CouchbaseConfigurations.PASSWORD.get(),
                OldCouchbaseConfigurations.PASSWORD.get()))
                .map(Object::toString).orElse(null);
    }

    protected List<String> getHosts(Settings settings) {
        return settings.prefix(asList(CouchbaseConfigurations.HOST.get(),
                OldCouchbaseConfigurations.HOST.get(), Configurations.HOST.get()))
                .stream().map(Object::toString).collect(toList());
    }

    /**
     * Adds a new node to cluster
     *
     * @param node the new node
     * @throws NullPointerException when the cluster is null
     */
    public void add(String node) throws NullPointerException {
        nodes.add(Objects.requireNonNull(node, "node is required"));
    }

    /**
     * set the user
     *
     * @param user the user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * set the password
     *
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }
}
