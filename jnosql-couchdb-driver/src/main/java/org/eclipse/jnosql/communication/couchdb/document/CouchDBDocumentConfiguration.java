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
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.document.DocumentConfiguration;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * The CouchDB implementation of {@link DocumentConfiguration}  that returns
 * {@link CouchDBDocumentCollectionManagerFactory}.
 * @see CouchDBConfigurations
 */
public class CouchDBDocumentConfiguration implements DocumentConfiguration {


    private static final String FILE_CONFIGURATION = "couchdb.properties";

    @Override
    public CouchDBDocumentCollectionManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        SettingsBuilder builder = Settings.builder();
        configuration.forEach((key, value) -> builder.put(key, value));
        return get(builder.build());
    }

    @Override
    public CouchDBDocumentCollectionManagerFactory get(Settings settings) {
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
        settings.computeIfPresent(CouchDBConfigurations.PORT, (k, v) -> configuration.withPort(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.MAX_CONNECTIONS, (k, v) -> configuration.withMaxConnections(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.CONNECTION_TIMEOUT, (k, v) -> configuration.withConnectionTimeout(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.SOCKET_TIMEOUT, (k, v) -> configuration.withSocketTimeout(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.MAX_OBJECT_SIZE_BYTES, (k, v) -> configuration.withMaxObjectSizeBytes(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.MAX_CACHE_ENTRIES, (k, v) -> configuration.withMaxCacheEntries(Integer.parseInt(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.ENABLE_SSL, (k, v) -> configuration.withEnableSSL(Boolean.parseBoolean(v.toString())));
        settings.computeIfPresent(CouchDBConfigurations.COMPRESSION, (k, v) -> configuration.withCompression(Boolean.parseBoolean(v.toString())));
        return new CouchDBDocumentCollectionManagerFactory(configuration.build());
    }
}