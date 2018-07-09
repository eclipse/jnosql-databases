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

import org.jnosql.diana.api.JNoSQLException;

import java.net.MalformedURLException;
import java.net.URL;

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
        URL url = getUrl(host);
        this.host = url.getHost();
        this.port = url.getPort();
        if (url.getUserInfo() != null) {
            String[] userInfoParts = url.getUserInfo().split(":");
            if (userInfoParts.length == 2) {
                this.username = userInfoParts[0];
                this.password = userInfoParts[1];
            }
        }
        withEnableSSL("https".equals(url.getProtocol()));

        if (this.port == -1) {
            if (this.enableSSL) {
                this.port = 443;
            } else {
                this.port = 80;
            }
        }
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

    public CouchDBHttpConfigurationBuilder withEnableSSL(boolean enableSSL) {
        this.enableSSL = enableSSL;
        return this;
    }

    public CouchDBHttpConfiguration build() {
        return new CouchDBHttpConfiguration(host, port, maxConnections, connectionTimeout,
                socketTimeout, proxyPort, proxy, enableSSL, username, password,
                compression,
                maxObjectSizeBytes, maxCacheEntries);
    }


    private URL getUrl(String host) {
        try {
            return new URL(host);
        } catch (MalformedURLException exp) {
            throw new JNoSQLException("There is an error when load the host at couchDB", exp);
        }

    }
}