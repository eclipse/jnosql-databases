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

public enum  CouchDBConfigurations implements Supplier<String> {

    PORT("couchdb.port"),
    MAX_CONNECTIONS("couchdb.max.connections"),
    CONNECTION_TIMEOUT("couchdb.connection.timeout"),
    SOCKET_TIMEOUT("couchdb.socket.timeout"),
    MAX_OBJECT_SIZE_BYTES("couchdb.max.object.size.bytes"),
    MAX_CACHE_ENTRIES("couchdb.max.cache.entries"),
    HOST("couchdb.host"),
    USER("couchdb.username"),
    PASSWORD("couchdb.password"),
    ENABLE_SSL("couchdb.enable.ssl"),
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

