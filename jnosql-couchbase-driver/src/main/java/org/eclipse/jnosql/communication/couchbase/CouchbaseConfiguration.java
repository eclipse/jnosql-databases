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
package org.eclipse.jnosql.communication.couchbase;


import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;

/**
 * The configuration base to all configuration implementation on couchbase
 */
public abstract class CouchbaseConfiguration {

    private static final String FILE_CONFIGURATION = "couchbase.properties";

    protected String host;

    protected String user;

    protected String password;

    protected String scope;

    protected List<String> collections = new ArrayList<>();

    protected String index;

    public CouchbaseConfiguration() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        SettingsBuilder builder = Settings.builder();
        configuration.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        Settings settings = builder.build();
        update(settings);
    }

    protected void update(Settings settings) {
        this.host = getHost(settings);
        this.user = getUser(settings);
        this.password = getPassword(settings);
        this.scope = getScope(settings);
        this.collections = getCollections(settings);
        this.index = getIndex(settings);
    }

    protected String getUser(Settings settings) {
        return settings.get(asList(Configurations.USER.get(),
                        CouchbaseConfigurations.USER.get()))
                .map(Object::toString).orElse(null);
    }

    private String getScope(Settings settings) {
        return settings.get(CouchbaseConfigurations.SCOPE.get())
                .map(Object::toString).orElse(null);
    }

    private String getIndex(Settings settings) {
        return settings.get(CouchbaseConfigurations.INDEX.get())
                .map(Object::toString).orElse(null);
    }

    private List<String> getCollections(Settings settings) {
        List<String> collections = new ArrayList<>();
        settings.get(CouchbaseConfigurations.COLLECTIONS.get())
                .map(Object::toString).stream()
                .flatMap(s -> Stream.of(s.split(",\\s*")))
                .forEach(collections::add);
        return collections;
    }

    protected String getPassword(Settings settings) {

        return settings.get(asList(Configurations.PASSWORD.get(),
                        CouchbaseConfigurations.PASSWORD.get()))
                .map(Object::toString).orElse(null);
    }

    protected String getHost(Settings settings) {
        return settings.get(asList(Configurations.HOST.get(),
                        CouchbaseConfigurations.HOST.get()))
                .map(Object::toString).orElse(null);
    }


    /**
     * set the host
     *
     * @param host the host
     */
    public void setHost(String host) {
        this.host = host;
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
