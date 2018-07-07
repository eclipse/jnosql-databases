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

import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.api.document.DocumentConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.Map;
import java.util.Objects;

/**
 * The CouchDB implementation of {@link DocumentConfiguration} that returns
 * {@link CouchDBDocumentCollectionManagerFactory}.
 * <p>couchdb-port: the port</p>
 * <p>couchbase-user: the user</p>
 * <p>couchbase-PASSWORD: the PASSWORD</p>
 */
public class CouchDBDocumentConfiguration implements DocumentConfiguration<CouchDBDocumentCollectionManagerFactory> {

    public static final String PORT = "couchdb-port";
    public static final String MAX_CONNECTIONS = "couchdb-MAX_CONNECTIONS";
    public static final String CONNECTION_TIMEOUT = "couchdb-CONNECTION_TIMEOUT";
    public static final String SOCKET_TIMEOUT = "couchdb-SOCKET_TIMEOUT";
    public static final String PROXY_PORT = "couchdb-PROXY_PORT";
    public static final String MAX_OBJECT_SIZE_BYTES = "couchdb-MAX_OBJECT_SIZE_BYTES";
    public static final String MAX_CACHE_ENTRIES = "couchdb-MAX_CACHE_ENTRIES";
    public static final String PROXY = "couchdb-PROXY";
    public static final String HOST = "couchdb-HOST";
    public static final String USERNAME = "couchdb-USERNAME";
    public static final String PASSWORD = "couchdb-PASSWORD";

    public static final String CLEANUP_IDLE_CONNECTIONS = "couchdb-CLEANUP_IDLE_CONNECTIONS";
    public static final String RELAXED_SSL_SETTINGS = "couchdb-RELAXED_SSL_SETTINGS";
    public static final String USE_EXPECT_CONTINUE = "couchdb-USE_EXPECT_CONTINUE";
    public static final String ENABLE_SSL = "couchdb-ENABLE_SSL";
    public static final String CACHING = "couchdb-CACHING";
    public static final String COMPRESSION = "couchdb-COMPRESSION";

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
        CouchDBHttpConfiguration configuration = new CouchDBHttpConfiguration();
        settings.computeIfPresent(PORT, (k,v) -> configuration.withPort(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_CONNECTIONS, (k,v) -> configuration.withMaxConnections(Integer.valueOf(v.toString())));
        settings.computeIfPresent(CONNECTION_TIMEOUT, (k,v) -> configuration.withConnectionTimeout(Integer.valueOf(v.toString())));
        settings.computeIfPresent(SOCKET_TIMEOUT, (k,v) -> configuration.withSocketTimeout(Integer.valueOf(v.toString())));
        settings.computeIfPresent(PROXY_PORT, (k,v) -> configuration.withProxyPort(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_OBJECT_SIZE_BYTES, (k,v) -> configuration.withMaxObjectSizeBytes(Integer.valueOf(v.toString())));
        settings.computeIfPresent(MAX_CACHE_ENTRIES, (k,v) -> configuration.withMaxCacheEntries(Integer.valueOf(v.toString())));
        settings.computeIfPresent(PROXY, (k,v) -> configuration.withProxy(v.toString()));
        settings.computeIfPresent(HOST, (k,v) -> configuration.withHost(v.toString()));
        settings.computeIfPresent(USERNAME, (k,v) -> configuration.withUsername(v.toString()));
        settings.computeIfPresent(PASSWORD, (k,v) -> configuration.withPassword(v.toString()));
        settings.computeIfPresent(CLEANUP_IDLE_CONNECTIONS, (k,v) -> configuration.withCleanupIdleConnections(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(RELAXED_SSL_SETTINGS, (k,v) -> configuration.withRelaxedSSLSettings(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(USE_EXPECT_CONTINUE, (k,v) -> configuration.withUseExpectContinue(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(ENABLE_SSL, (k,v) -> configuration.withEnableSSL(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(CACHING, (k,v) -> configuration.withCaching(Boolean.valueOf(v.toString())));
        settings.computeIfPresent(COMPRESSION, (k,v) -> configuration.withCompression(Boolean.valueOf(v.toString())));
        return new CouchDBDocumentCollectionManagerFactory(configuration.build());
    }
}