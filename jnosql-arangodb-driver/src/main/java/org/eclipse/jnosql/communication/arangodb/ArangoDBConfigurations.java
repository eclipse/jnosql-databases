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
package org.eclipse.jnosql.communication.arangodb;


import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the ArangoDB database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see jakarta.nosql.Settings
 */
public enum ArangoDBConfigurations implements Supplier<String> {


    /**
     * The database host, where you need to put the port split by colons. E.g., localhost:8529
     */
    HOST("arangodb.host"),
    /**
     * The user's credential.
     */
    USER("arangodb.user"),
    /**
     * The password's credential
     */
    PASSWORD("arangodb.password"),
    /**
     * The connection and request timeout in milliseconds.
     */
    TIMEOUT("arangodb.timeout"),
    /**
     * The chunk size when {@link com.arangodb.Protocol} is used.
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
     *Set hosts split by comma
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
