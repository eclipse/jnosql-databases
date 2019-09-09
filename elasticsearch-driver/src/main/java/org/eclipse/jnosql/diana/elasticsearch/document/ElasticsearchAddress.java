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
package org.eclipse.jnosql.diana.elasticsearch.document;

import org.apache.http.HttpHost;

final class ElasticsearchAddress {

    private final String host;

    private final int port;

    private ElasticsearchAddress(String address, int defaultPort) {
        String[] values = address.split(":");

        this.host = values[0];
        this.port = values.length == 2 ? Integer.valueOf(values[1]) : defaultPort;
    }

    public HttpHost toHttpHost() {
            return new HttpHost(host, port);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticsearchAddress{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    static ElasticsearchAddress of(String address, int defaultPort) {
        return new ElasticsearchAddress(address, defaultPort);
    }
}
