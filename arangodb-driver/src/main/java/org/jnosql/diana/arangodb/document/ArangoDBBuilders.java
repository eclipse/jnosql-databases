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
package org.jnosql.diana.arangodb.document;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;
import org.jnosql.diana.api.Settings;

import java.util.Arrays;
import java.util.List;

import static java.util.Optional.ofNullable;

final class ArangoDBBuilders {

    private static final String HOST = "arangodb-host";
    private static final String USER = "arangodb-user";
    private static final String PASSWORD = "arangodb-password";
    private static final String PORT = "arangodb-port";
    private static final String CHUCK_SIZE = "arangodb-chuckSize";
    private static final String TIMEOUT = "arangodb-timeout";
    private static final String USER_SSL = "arangodb-userSsl";
    private static final String LOAD_BALANCING_STRATEGY = "arangodb.loadBalancingStrategy";
    private static final String PROTOCOL = "arangodb.protocol";
    private static final String CHUNK_CONTENT_SIZE = "arangodb.chunksize";
    private static final String MAX_CONNECTIONS = "arangodb.connections.max";
    private static final String ACQUIRE_HOST_LIST = "arangodb.acquireHostList";
    private static final String KEY_HOSTS = "arangodb.hosts";

    private ArangoDBBuilders() {
    }

    public static void load(Settings settings, ArangoDBBuilder arangoDB) {

        ofNullable(settings.get(HOST)).map(Object::toString).ifPresent(arangoDB::host);
        ofNullable(settings.get(USER)).map(Object::toString).ifPresent(arangoDB::user);
        ofNullable(settings.get(PASSWORD)).map(Object::toString).ifPresent(arangoDB::password);
        ofNullable(settings.get(PORT)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get(CHUCK_SIZE)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get(TIMEOUT)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::timeout);

        ofNullable(settings.get(CHUNK_CONTENT_SIZE)).map(Object::toString).map(Integer::valueOf)
                .ifPresent(arangoDB::chunksize);
        ofNullable(settings.get(MAX_CONNECTIONS)).map(Object::toString).map(Integer::valueOf)
                .ifPresent(arangoDB::maxConnections);
        ofNullable(settings.get(USER_SSL)).map(Object::toString).map(Boolean::valueOf)
                .ifPresent(arangoDB::useSsl);
        ofNullable(settings.get(ACQUIRE_HOST_LIST)).map(Object::toString).map(Boolean::valueOf)
                .ifPresent(arangoDB::acquireHostList);
        ofNullable(settings.get(LOAD_BALANCING_STRATEGY)).map(Object::toString).map(LoadBalancingStrategy::valueOf)
                .ifPresent(arangoDB::loadBalancingStrategy);
        ofNullable(settings.get(PROTOCOL)).map(Object::toString).map(Protocol::valueOf)
                .ifPresent(arangoDB::useProtocol);

       ofNullable(settings.get(KEY_HOSTS)).map(Object::toString)
                .map(ArangoDBHost::new).map(ArangoDBHost::getHost)
                .ifPresent(l ->feed(l, arangoDB));

    }

    private static void feed(List<String> hosts, ArangoDBBuilder arangoDB) {
        for (String host : hosts) {
            final String[] values = host.split(":");
            arangoDB.host(values[0], Integer.valueOf(values[0]));
        }
    }
    private static class ArangoDBHost {
        private final String hots;

        private ArangoDBHost(String hots) {
            this.hots = hots;
        }

        public List<String> getHost() {
            return Arrays.asList(this.hots.split(","));
        }
    }
}
