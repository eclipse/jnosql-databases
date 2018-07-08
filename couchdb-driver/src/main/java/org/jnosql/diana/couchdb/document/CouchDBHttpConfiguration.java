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

class CouchDBHttpConfiguration {

    private final String host;
    private final int port;
    private final int maxConnections;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final int proxyPort;
    private final String proxy;
    private final boolean enableSSL;

    private final String username;
    private final String password;

    private final boolean relaxedSSLSettings;
    private final boolean cleanupIdleConnections;
    private final boolean useExpectContinue;
    private final boolean caching;
    private final boolean compression;
    private final int maxObjectSizeBytes;
    private final int maxCacheEntries;

    public CouchDBHttpConfiguration(String host, int port, int maxConnections,
                                    int connectionTimeout, int socketTimeout, int proxyPort,
                                    String proxy, boolean enableSSL, String username, String password,
                                    boolean relaxedSSLSettings, boolean cleanupIdleConnections,
                                    boolean useExpectContinue,
                                    boolean caching, boolean compression, int maxObjectSizeBytes,
                                    int maxCacheEntries) {
        this.host = host;
        this.port = port;
        this.maxConnections = maxConnections;
        this.connectionTimeout = connectionTimeout;
        this.socketTimeout = socketTimeout;
        this.proxyPort = proxyPort;
        this.proxy = proxy;
        this.enableSSL = enableSSL;
        this.username = username;
        this.password = password;
        this.relaxedSSLSettings = relaxedSSLSettings;
        this.cleanupIdleConnections = cleanupIdleConnections;
        this.useExpectContinue = useExpectContinue;
        this.caching = caching;
        this.compression = compression;
        this.maxObjectSizeBytes = maxObjectSizeBytes;
        this.maxCacheEntries = maxCacheEntries;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public int getProxyPort() {
        return proxyPort;
    }

    public String getProxy() {
        return proxy;
    }

    public boolean isEnableSSL() {
        return enableSSL;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public boolean isRelaxedSSLSettings() {
        return relaxedSSLSettings;
    }

    public boolean isCleanupIdleConnections() {
        return cleanupIdleConnections;
    }

    public boolean isUseExpectContinue() {
        return useExpectContinue;
    }

    public boolean isCaching() {
        return caching;
    }

    public boolean isCompression() {
        return compression;
    }

    public int getMaxObjectSizeBytes() {
        return maxObjectSizeBytes;
    }

    public int getMaxCacheEntries() {
        return maxCacheEntries;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CouchDBHttpConfiguration{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append(", maxConnections=").append(maxConnections);
        sb.append(", connectionTimeout=").append(connectionTimeout);
        sb.append(", socketTimeout=").append(socketTimeout);
        sb.append(", proxyPort=").append(proxyPort);
        sb.append(", proxy='").append(proxy).append('\'');
        sb.append(", enableSSL=").append(enableSSL);
        sb.append(", username='").append(username).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", relaxedSSLSettings=").append(relaxedSSLSettings);
        sb.append(", cleanupIdleConnections=").append(cleanupIdleConnections);
        sb.append(", useExpectContinue=").append(useExpectContinue);
        sb.append(", caching=").append(caching);
        sb.append(", compression=").append(compression);
        sb.append(", maxObjectSizeBytes=").append(maxObjectSizeBytes);
        sb.append(", maxCacheEntries=").append(maxCacheEntries);
        sb.append('}');
        return sb.toString();
    }
}
