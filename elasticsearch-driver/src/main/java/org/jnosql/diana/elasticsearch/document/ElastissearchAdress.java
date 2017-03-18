/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package org.jnosql.diana.elasticsearch.document;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;

final class ElastissearchAdress {

    private final String host;

    private final int port;

    private ElastissearchAdress(String address, int defaultPort) {
        String[] values = address.split(":");

        this.host = values[0];
        this.port = values.length == 2 ? Integer.valueOf(values[1]) : defaultPort;
    }

    public TransportAddress toTransportAddress() {
        try {
            return new InetSocketTransportAddress(InetAddress.getByName(host), port);
        } catch (UnknownHostException e) {
            throw new ElasticsearchException("An error when try to load the address: " + host, e);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ElastissearchAdress{");
        sb.append("host='").append(host).append('\'');
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    static ElastissearchAdress of(String address, int defaultPort) {
        return new ElastissearchAdress(address, defaultPort);
    }
}
