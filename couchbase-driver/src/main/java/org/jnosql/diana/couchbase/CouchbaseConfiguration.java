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


import org.jnosql.diana.driver.ConfigurationReader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The configuration base to all configuration implementation on couchbase
 */
public abstract class CouchbaseConfiguration {

    private static final String FILE_CONFIGURATION = "diana-couchbase.properties";
    protected static final String COUCHBASE_HOST = "couchbase-host-";
    protected static final String COUCHBASE_USER = "couchbase-user";
    protected static final String COUCHBASE_PASSWORD = "couchbase-password";

    protected final List<String> nodes = new ArrayList<>();

    protected String user;

    protected String password;

    public CouchbaseConfiguration() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        configuration.keySet()
                .stream()
                .filter(k -> k.startsWith(COUCHBASE_HOST))
                .sorted()
                .map(configuration::get)
                .forEach(this::add);
        this.user = configuration.get(COUCHBASE_USER);
        this.password = configuration.get(COUCHBASE_PASSWORD);
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
     * @param user the user
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * set the password
     * @param password the password
     */
    public void setPassword(String password) {
        this.password = password;
    }
}
