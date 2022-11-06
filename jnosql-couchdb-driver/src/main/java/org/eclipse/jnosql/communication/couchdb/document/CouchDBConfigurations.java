/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document;

import java.util.function.Supplier;

/**
 * This class is a {@link Supplier} of properties settings available at Couchdb client.
 */
public enum  CouchDBConfigurations implements Supplier<String> {

    /**
     * The port connection to a client connect. The default value is "5984"
     */
    PORT("couchdb.port"),

    /**
     * The max of connection that the couchdb client have. The default value is "20"
     */
    MAX_CONNECTIONS("couchdb.max.connections"),

    /**
     * The timeout in milliseconds used when requesting a connection. The default value is "1000".
     */
    CONNECTION_TIMEOUT("couchdb.connection.timeout"),

    /**
     * The socket timeout in milliseconds, which is the timeout for waiting for data or, put differently,
     *  a maximum period inactivity between two consecutive data packets). The default value is "10000".
     */
    SOCKET_TIMEOUT("couchdb.socket.timeout"),

    /**
     *  The current maximum response body size that will be cached. The value is "8192".
     */
    MAX_OBJECT_SIZE_BYTES("couchdb.max.object.size.bytes"),

    /**
     * The maximum number of cache entries the cache will retain. The default value is "1000".
     */
    MAX_CACHE_ENTRIES("couchdb.max.cache.entries"),
    /**
     * The host at the database.
     */
    HOST("couchdb.host"),
    /**
     * The user's credential.
     */
    USER("couchdb.username"),

    /**
     * The password's credential
     */
    PASSWORD("couchdb.password"),
    /**
     *If the request will use a https or a http.
     */
    ENABLE_SSL("couchdb.enable.ssl"),

    /**
     * Determines whether compressed entities should be decompressed automatically.
     */
    COMPRESSION("couchdb.compression");

    private final String configuration;

    CouchDBConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}

