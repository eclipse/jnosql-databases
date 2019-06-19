/*
 *
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
 *
 */
package org.jnosql.diana.couchdb.document;

import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.document.DocumentConfiguration;
import jakarta.nosql.document.DocumentConfigurationAsync;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.COMPRESSION;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.CONNECTION_TIMEOUT;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.ENABLE_SSL;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.HOST;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.MAX_CACHE_ENTRIES;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.MAX_CONNECTIONS;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.MAX_OBJECT_SIZE_BYTES;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.PASSWORD;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.PORT;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.SOCKET_TIMEOUT;
import static org.jnosql.diana.couchdb.document.CouchDBConfigurations.USER;

/**
 * The CouchDB implementation of {@link DocumentConfiguration} and {@link DocumentConfigurationAsync} that returns
 * {@link CouchDBDocumentCollectionManagerFactory}, settings:
 * <p>couchdb.port: </p>
 * <p>couchdb.max.connections: </p>
 * <p>couchdb.connection.timeout: </p>
 * <p>couchdb.socket.timeout: </p>
 * <p>couchdb.max.object.size.bytes: </p>
 * <p>couchdb.max.cache.entries: </p>
 * <p>couchdb.host: </p>
 * <p>couchdb.username: </p>
 * <p>couchdb.password: </p>
 * <p>couchdb.enable.ssl: </p>
 * <p>couchdb.compression: </p>
 *
 * @see CouchDBConfigurations
 */
public class CouchDBDocumentConfiguration implements DocumentConfiguration, DocumentConfigurationAsync {


    private static final String FILE_CONFIGURATION = "diana-couchdb.properties";

    @Override
    public CouchDBDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        SettingsBuilder builder = Settings.builder();
        configuration.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }

    @Override
    public CouchDBDocumentCollectionManagerFactory get(Settings settings) {
        Objects.requireNonNull(settings, "settings is required");
        CouchDBHttpConfigurationBuilder configuration = new CouchDBHttpConfigurationBuilder();

        settings.get(Arrays.asList(HOST.get(), Configurations.HOST.get()))
                .map(Object::toString)
                .ifPresent(configuration::withHost);
        settings.get(Arrays.asList(USER.get(), Configurations.USER.get()))
                .map(Object::toString)
                .ifPresent(configuration::withUsername);
        settings.get(Arrays.asList(PASSWORD.get(), Configurations.PASSWORD.get()))
                .map(Object::toString)
                .ifPresent(configuration::withPassword);
        settings.computeIfPresent(PORT.get(), (k, v) -> configuration.withPort(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_CONNECTIONS.get(), (k, v) -> configuration.withMaxConnections(Integer.valueOf(v.toString())));
        settings.computeIfPresent(CONNECTION_TIMEOUT.get(), (k, v) -> configuration.withConnectionTimeout(Integer.valueOf(v.toString())));
        settings.computeIfPresent(SOCKET_TIMEOUT.get(), (k, v) -> configuration.withSocketTimeout(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_OBJECT_SIZE_BYTES.get(), (k, v) -> configuration.withMaxObjectSizeBytes(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_CACHE_ENTRIES.get(), (k, v) -> configuration.withMaxCacheEntries(Integer.valueOf(v.toString())));
        settings.computeIfPresent(ENABLE_SSL.get(), (k, v) -> configuration.withEnableSSL(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(COMPRESSION.get(), (k, v) -> configuration.withCompression(Boolean.valueOf(v.toString())));
        return new CouchDBDocumentCollectionManagerFactory(configuration.build());
    }
}