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

import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.cache.CacheConfig;
import org.apache.http.impl.client.cache.CachingHttpClients;

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


    private final boolean compression;
    private final int maxObjectSizeBytes;
    private final int maxCacheEntries;
    private final String url;


    CouchDBHttpConfiguration(String host, int port, int maxConnections,
                             int connectionTimeout, int socketTimeout, int proxyPort,
                             String proxy, boolean enableSSL, String username, String password,
                             boolean compression, int maxObjectSizeBytes,
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
        this.compression = compression;
        this.maxObjectSizeBytes = maxObjectSizeBytes;
        this.maxCacheEntries = maxCacheEntries;
        this.url = createUrl();
    }

    private String createUrl() {
        StringBuilder url = new StringBuilder();
        if (enableSSL) {
            url.append("https;//");
        } else {
            url.append("http;//");
        }
        url.append(':').append(port).append('/');
        return url.toString();
    }

    public CouchDBHttpClient getClient(String database) {
        return new CouchDBHttpClient(this, getHttpClient(), database);
    }

    public String getUrl() {
        return url;
    }


    private CloseableHttpClient getHttpClient() {
        CacheConfig cacheConfig = CacheConfig.custom()
                .setMaxCacheEntries(maxCacheEntries)
                .setMaxObjectSize(maxObjectSizeBytes)
                .build();
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout)
                .setContentCompressionEnabled(compression)
                .build();
        HttpClientBuilder builder = CachingHttpClients.custom()
                .setCacheConfig(cacheConfig)
                .setDefaultRequestConfig(requestConfig);

        if (username != null) {
            CredentialsProvider provider = new BasicCredentialsProvider();
            UsernamePasswordCredentials credentials
                    = new UsernamePasswordCredentials(username, password);
            provider.setCredentials(AuthScope.ANY, credentials);
            builder.setDefaultCredentialsProvider(provider);
        }

        return builder.build();

    }


}
