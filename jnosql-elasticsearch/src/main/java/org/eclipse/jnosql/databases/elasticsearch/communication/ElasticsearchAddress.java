/*
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
 *   Alessandro Moscatelli
 */
package org.eclipse.jnosql.databases.elasticsearch.communication;

import org.apache.http.HttpHost;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Objects;

final class ElasticsearchAddress {

    private HttpHost host;

    private ElasticsearchAddress(String address, int defaultPort) {
        try {
            URL tmp = new URL(address);
            this.host = new HttpHost(
                    tmp.getHost(),
                    Objects.equals(tmp.getPort(), -1) ? defaultPort : tmp.getPort(),
                    tmp.getProtocol()
            );
        } catch (MalformedURLException ex) {
            String[] values = address.split(":");
            this.host = new HttpHost(
                    values[0],
                    values.length == 2 ? Integer.parseInt(values[1]) : defaultPort
            );
        }
    }

    public HttpHost toHttpHost() {
        return this.host;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElasticsearchAddress{");
        sb.append("host='").append(host.getHostName()).append('\'');
        sb.append(", port=").append(host.getPort());
        sb.append('}');
        return sb.toString();
    }

    static ElasticsearchAddress of(String address, int defaultPort) {
        return new ElasticsearchAddress(address, defaultPort);
    }
}
