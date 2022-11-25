/*
 *
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
 *
 */
package org.eclipse.jnosql.communication.couchdb.document;

import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.document.DocumentConfiguration;

import java.util.Arrays;
import java.util.Objects;

import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.COMPRESSION;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.CONNECTION_TIMEOUT;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.ENABLE_SSL;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.MAX_CACHE_ENTRIES;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.MAX_CONNECTIONS;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.MAX_OBJECT_SIZE_BYTES;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.PORT;
import static org.eclipse.jnosql.communication.couchdb.document.CouchDBConfigurations.SOCKET_TIMEOUT;

/**
 * The CouchDB implementation of {@link DocumentConfiguration}  that returns
 * {@link CouchDBDocumentManagerFactory}.
 * @see CouchDBConfigurations
 */
public class CouchDBDocumentConfiguration implements DocumentConfiguration {



    @Override
    public CouchDBDocumentManagerFactory apply(Settings settings) {
        Objects.requireNonNull(settings, "settings is required");
        CouchDBHttpConfigurationBuilder configuration = new CouchDBHttpConfigurationBuilder();

        settings.getSupplier(Arrays.asList(CouchDBConfigurations.HOST, Configurations.HOST))
                .map(Object::toString)
                .ifPresent(configuration::withHost);
        settings.getSupplier(Arrays.asList(CouchDBConfigurations.USER, Configurations.USER))
                .map(Object::toString)
                .ifPresent(configuration::withUsername);
        settings.getSupplier(Arrays.asList(CouchDBConfigurations.PASSWORD, Configurations.PASSWORD))
                .map(Object::toString)
                .ifPresent(configuration::withPassword);

        settings.get(PORT, Integer.class).ifPresent(configuration::withPort);
        settings.get(MAX_CONNECTIONS, Integer.class).ifPresent(configuration::withMaxConnections);
        settings.get(CONNECTION_TIMEOUT, Integer.class).ifPresent(configuration::withConnectionTimeout);

        settings.get(SOCKET_TIMEOUT, Integer.class).ifPresent(configuration::withSocketTimeout);
        settings.get(MAX_OBJECT_SIZE_BYTES, Integer.class).ifPresent(configuration::withMaxObjectSizeBytes);
        settings.get(MAX_CACHE_ENTRIES, Integer.class).ifPresent(configuration::withMaxCacheEntries);

        settings.get(ENABLE_SSL, Boolean.class).ifPresent(configuration::withEnableSSL);
        settings.get(COMPRESSION, Boolean.class).ifPresent(configuration::withCompression);

        return new CouchDBDocumentManagerFactory(configuration.build());
    }
}