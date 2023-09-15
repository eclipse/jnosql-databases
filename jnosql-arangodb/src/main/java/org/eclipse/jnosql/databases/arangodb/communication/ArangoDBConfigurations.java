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
package org.eclipse.jnosql.databases.arangodb.communication;


import java.util.function.Supplier;

/**
 * An enumeration representing various configuration options for connecting to the ArangoDB database.
 * This enum implements the {@link Supplier} interface and provides the property name that can be
 * overridden by the system environment using Eclipse MicroProfile or Jakarta Config API.
 *
 * @see org.eclipse.jnosql.communication.Settings
 */
public enum ArangoDBConfigurations implements Supplier<String> {


    /**
     * The database host, including the port number separated by a colon. For example: jnosql.arangodb.host=localhost:8529
     */
    HOST("jnosql.arangodb.host"),

    /**
     * The username used for authentication.
     */
    USER("jnosql.arangodb.user"),

    /**
     * The password used for authentication.
     */
    PASSWORD("jnosql.arangodb.password"),

    /**
     * The connection and request timeout in milliseconds.
     */
    TIMEOUT("jnosql.arangodb.timeout"),

    /**
     * The chunk size when using the {@link com.arangodb.Protocol}.
     */
    CHUNK_SIZE("jnosql.arangodb.chunk.size"),

    /**
     * Specifies whether SSL should be enabled when connecting to an ArangoDB server.
     */
    USER_SSL("jnosql.arangodb.user.ssl"),

    /**
     * The load balancing strategy to be used. See {@link com.arangodb.entity.LoadBalancingStrategy}.
     */
    LOAD_BALANCING("jnosql.arangodb.load.balancing.strategy"),

    /**
     * The protocol to be used. See {@link com.arangodb.Protocol}.
     */
    PROTOCOL("jnosql.arangodb.protocol"),

    /**
     * The maximum number of connections the built-in connection pool will open per host.
     */
    MAX_CONNECTIONS("jnosql.arangodb.connections.max"),

    /**
     * A comma-separated list of host addresses.
     */
    HOST_LIST("jnosql.arangodb.acquire.host.list"),

    /**
     * Define the list of deserializer classes that will be used by ArangoDB.
     * These classes must extend {@link com.fasterxml.jackson.databind.JsonSerializer}.
     * Example: jnosql.arangodb.deserializer.1=my.package.MyCustomSerializer
     */
    SERIALIZER("jnosql.arangodb.serializer"),

    /**
     * Define the list of deserializer classes that will be used by ArangoDB.
     * These classes must extend {@link com.fasterxml.jackson.databind.JsonDeserializer}.
     * Example: jnosql.arangodb.deserializer.1=my.package.MyCustomDeserializer
     */
    DESERIALIZER("jnosql.arangodb.deserializer");
    private final String configuration;

    ArangoDBConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
