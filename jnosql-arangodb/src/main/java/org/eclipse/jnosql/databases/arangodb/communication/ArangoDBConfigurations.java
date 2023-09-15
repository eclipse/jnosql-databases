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
 * An enumeration to show the available options to connect to the ArangoDB database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see org.eclipse.jnosql.communication.Settings
 */
public enum ArangoDBConfigurations implements Supplier<String> {


    /**
     * The database host, where you need to put the port split by colons. E.g.: jnosql.arangodb.host=localhost:8529
     */
    HOST("jnosql.arangodb.host"),
    /**
     * The user's credential.
     */
    USER("jnosql.arangodb.user"),
    /**
     * The password's credential
     */
    PASSWORD("jnosql.arangodb.password"),
    /**
     * The connection and request timeout in milliseconds.
     */
    TIMEOUT("jnosql.arangodb.timeout"),
    /**
     * The chunk size when {@link com.arangodb.Protocol} is used.
     */
    CHUNK_SIZE("jnosql.arangodb.chunk.size"),
    /**
     * The true SSL will be used when connecting to an ArangoDB server.
     */
    USER_SSL("jnosql.arangodb.user.ssl"),
    /**
     *The {@link com.arangodb.entity.LoadBalancingStrategy}
     */
    LOAD_BALANCING("jnosql.arangodb.load.balancing.strategy"),
    /**
     * The {@link com.arangodb.Protocol}
     */
    PROTOCOL("jnosql.arangodb.protocol"),
    /**
     * The maximum number of connections the built-in connection pool will open per host.
     */
    MAX_CONNECTIONS("jnosql.arangodb.connections.max"),
    /**
     *Set hosts split by comma
     */
    HOST_LIST("jnosql.arangodb.acquire.host.list"),
    /*
     * Define the list of serializer classes that will be set in the ArangoDB.
     * Those classes must extend {@link com.fasterxml.jackson.databind.JsonSerializer}. E.g.: jnosql.arangodb.serializer.1=my.pacage.MyCustomSerializer
     */
    SERIALIZER("jnosql.arangodb.serializer"),
    /*
     * Define the list of deserializer classes that will be set in the ArangoDB.
     * Those classes must extend {@link com.fasterxml.jackson.databind.JsonDeserializer}. E.g.: jnosql.arangodb.deserializer.1=my.pacage.MyCustomDeserializer
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
