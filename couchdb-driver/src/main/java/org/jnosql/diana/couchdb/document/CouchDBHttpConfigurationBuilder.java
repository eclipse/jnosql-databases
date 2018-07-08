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

class CouchDBHttpConfigurationBuilder {

    private String host = "localhost";
    private int port = 5984;
    private int maxConnections = 20;
    private int connectionTimeout = 1000;
    private int socketTimeout = 10000;
    private int proxyPort = -1;
    private String proxy = null;
    private boolean enableSSL = false;

    private String username;
    private String password;

    private boolean relaxedSSLSettings = false;
    private boolean cleanupIdleConnections = true;
    private boolean useExpectContinue = true;
    private boolean caching = true;
    private boolean compression = false;
    private int maxObjectSizeBytes = 8192;
    private int maxCacheEntries = 1000;

    public CouchDBHttpConfigurationBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withProxyPort(int proxyPort) {
        this.proxyPort = proxyPort;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withMaxObjectSizeBytes(int maxObjectSizeBytes) {
        this.maxObjectSizeBytes = maxObjectSizeBytes;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withMaxCacheEntries(int maxCacheEntries) {
        this.maxCacheEntries = maxCacheEntries;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withProxy(String proxy) {
        this.proxy = proxy;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withHost(String host) {
        this.host = host;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withUsername(String username) {
        this.username = username;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withPassword(String password) {
        this.password = password;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withCleanupIdleConnections(boolean cleanupIdleConnections) {
        this.cleanupIdleConnections = cleanupIdleConnections;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withRelaxedSSLSettings(boolean relaxedSSLSettings) {
        this.relaxedSSLSettings =relaxedSSLSettings;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withUseExpectContinue(boolean useExpectContinue) {
        this.useExpectContinue = useExpectContinue;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withCaching(boolean caching) {
        this.caching = caching;
        return this;
    }

    public CouchDBHttpConfigurationBuilder withCompression(boolean compression) {
        this.compression = compression;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchDBHttpConfigurationBuilder{");
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
