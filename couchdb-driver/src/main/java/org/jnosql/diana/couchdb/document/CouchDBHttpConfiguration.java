/*
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
 */
package org.jnosql.diana.couchdb.document;

import org.ektorp.http.HttpClient;
import org.ektorp.http.StdHttpClient;

import java.util.Optional;
import java.util.OptionalInt;

class CouchDBHttpConfiguration {

    private OptionalInt port = OptionalInt.empty();
    private OptionalInt maxConnections = OptionalInt.empty();
    private OptionalInt connectionTimeout = OptionalInt.empty();
    private OptionalInt socketTimeout = OptionalInt.empty();
    private OptionalInt proxyPort = OptionalInt.empty();
    private OptionalInt maxObjectSizeBytes = OptionalInt.empty();
    private OptionalInt maxCacheEntries = OptionalInt.empty();
    private Optional<String> proxy = Optional.empty();
    private Optional<String> host = Optional.empty();
    private Optional<String> username = Optional.empty();
    private Optional<String> password = Optional.empty();

    private Optional<Boolean> cleanupIdleConnections = Optional.empty();
    private Optional<Boolean> relaxedSSLSettings = Optional.empty();
    private Optional<Boolean> useExpectContinue = Optional.empty();
    private Optional<Boolean> enableSSL = Optional.empty();
    private Optional<Boolean> caching = Optional.empty();
    private Optional<Boolean> compression = Optional.empty();

    public CouchDBHttpConfiguration withPort(int port) {
        this.port = OptionalInt.of(port);
        return this;
    }

    public CouchDBHttpConfiguration withMaxConnections(int maxConnections) {
        this.maxConnections = OptionalInt.of(maxConnections);
        return this;
    }

    public CouchDBHttpConfiguration withConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = OptionalInt.of(connectionTimeout);
        return this;
    }

    public CouchDBHttpConfiguration withSocketTimeout(int socketTimeout) {
        this.socketTimeout = OptionalInt.of(socketTimeout);
        return this;
    }

    public CouchDBHttpConfiguration withProxyPort(int proxyPort) {
        this.proxyPort = OptionalInt.of(proxyPort);
        return this;
    }

    public CouchDBHttpConfiguration withMaxObjectSizeBytes(int maxObjectSizeBytes) {
        this.maxObjectSizeBytes = OptionalInt.of(maxObjectSizeBytes);
        return this;
    }

    public CouchDBHttpConfiguration withMaxCacheEntries(int maxCacheEntries) {
        this.maxCacheEntries = OptionalInt.of(maxCacheEntries);
        return this;
    }

    public CouchDBHttpConfiguration withProxy(String proxy) {
        this.proxy = Optional.ofNullable(proxy);
        return this;
    }

    public CouchDBHttpConfiguration withHost(String host) {
        this.host = Optional.ofNullable(host);
        return this;
    }

    public CouchDBHttpConfiguration withUsername(String username) {
        this.username = Optional.ofNullable(username);
        return this;
    }

    public CouchDBHttpConfiguration withPassword(String password) {
        this.password = Optional.ofNullable(password);
        return this;
    }

    public CouchDBHttpConfiguration withCleanupIdleConnections(boolean cleanupIdleConnections) {
        this.cleanupIdleConnections = Optional.of(cleanupIdleConnections);
        return this;
    }

    public CouchDBHttpConfiguration withRelaxedSSLSettings(boolean relaxedSSLSettings) {
        this.relaxedSSLSettings = Optional.of(relaxedSSLSettings);
        return this;
    }

    public CouchDBHttpConfiguration withUseExpectContinue(boolean useExpectContinue) {
        this.useExpectContinue = Optional.of(useExpectContinue);
        return this;
    }

    public CouchDBHttpConfiguration withEnableSSL(boolean enableSSL) {
        this.enableSSL = Optional.of(enableSSL);
        return this;
    }

    public CouchDBHttpConfiguration withCaching(boolean caching) {
        this.caching = Optional.of(caching);
        return this;
    }

    public CouchDBHttpConfiguration withCompression(boolean compression) {
        this.compression = Optional.of(compression);
        return this;
    }

    public HttpClient build() {
        StdHttpClient.Builder builder = new StdHttpClient.Builder();
        port.ifPresent(builder::port);
        maxConnections.ifPresent(builder::maxConnections);
        connectionTimeout.ifPresent(builder::connectionTimeout);
        socketTimeout.ifPresent(builder::socketTimeout);
        proxyPort.ifPresent(builder::proxyPort);
        maxObjectSizeBytes.ifPresent(builder::maxObjectSizeBytes);
        maxCacheEntries.ifPresent(builder::maxCacheEntries);
        proxy.ifPresent(builder::proxy);
        host.ifPresent(builder::host);
        username.ifPresent(builder::username);
        password.ifPresent(builder::password);
        cleanupIdleConnections.ifPresent(builder::cleanupIdleConnections);
        relaxedSSLSettings.ifPresent(builder::relaxedSSLSettings);
        useExpectContinue.ifPresent(builder::useExpectContinue);
        enableSSL.ifPresent(builder::enableSSL);
        caching.ifPresent(builder::caching);
        compression.ifPresent(builder::compression);
        return builder.build();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchDBHttpConfiguration{");
        sb.append("port=").append(port);
        sb.append(", maxConnections=").append(maxConnections);
        sb.append(", connectionTimeout=").append(connectionTimeout);
        sb.append(", socketTimeout=").append(socketTimeout);
        sb.append(", proxyPort=").append(proxyPort);
        sb.append(", maxObjectSizeBytes=").append(maxObjectSizeBytes);
        sb.append(", maxCacheEntries=").append(maxCacheEntries);
        sb.append(", proxy=").append(proxy);
        sb.append(", host=").append(host);
        sb.append(", username=").append(username);
        sb.append(", password=").append(password);
        sb.append(", cleanupIdleConnections=").append(cleanupIdleConnections);
        sb.append(", relaxedSSLSettings=").append(relaxedSSLSettings);
        sb.append(", useExpectContinue=").append(useExpectContinue);
        sb.append(", enableSSL=").append(enableSSL);
        sb.append(", caching=").append(caching);
        sb.append(", compression=").append(compression);
        sb.append('}');
        return sb.toString();
    }
}
