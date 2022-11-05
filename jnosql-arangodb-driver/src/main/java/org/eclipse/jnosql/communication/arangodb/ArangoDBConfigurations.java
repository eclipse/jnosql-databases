/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.eclipse.jnosql.communication.arangodb;


import java.util.function.Supplier;

/**
 * The settings option available at AragonDB
 *
 * @see jakarta.nosql.Settings
 */
public enum ArangoDBConfigurations implements Supplier<String> {


    /**
     * The database host, where you need to put the port split by colons. E.g., localhost:8529
     */
    HOST("arangodb.host"),
    /**
     * The user credential.
     */
    USER("arangodb.user"),
    /**
     * the password's credential
     */
    PASSWORD("arangodb.password"),
    /**
     * the connection and request timeout in milliseconds.
     */
    TIMEOUT("arangodb.timeout"),
    /**
     * the chunk size when {@link com.arangodb.Protocol#VST} is used.
     */
    CHUCK_SIZE("arangodb.chuck.size"),
    /**
     * The true SSL will be used when connecting to an ArangoDB server.
     */
    USER_SSL("arangodb.user.ssl"),
    /**
     *The {@link com.arangodb.entity.LoadBalancingStrategy}
     */
    LOAD_BALANCING("arangodb.load.balancing.strategy"),
    /**
     * The {@link com.arangodb.Protocol}
     */
    PROTOCOL("arangodb.protocol"),
    /**
     * The maximum number of connections the built in connection pool will open per host.
     */
    MAX_CONNECTIONS("arangodb.connections.max"),
    /**
     *Set host split by comma
     */
    HOST_LIST("arangodb.acquire.host.list"),
    FILE_CONFIGURATION("diana-arangodb.properties");
    private final String configuration;

    ArangoDBConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
