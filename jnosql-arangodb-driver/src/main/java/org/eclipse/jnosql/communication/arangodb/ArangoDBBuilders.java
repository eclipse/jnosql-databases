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
package org.eclipse.jnosql.communication.arangodb;

import com.arangodb.Protocol;
import com.arangodb.entity.LoadBalancingStrategy;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;

final class ArangoDBBuilders {


    private ArangoDBBuilders() {
    }

    static void load(Settings settings, ArangoDBBuilder arangoDB) {

        settings.get(asList(ArangoDBConfigurations.USER.get(), Configurations.USER.get()))
                .map(Object::toString).ifPresent(arangoDB::user);
        settings.get(asList(ArangoDBConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                .map(Object::toString).ifPresent(arangoDB::password);
        settings.get(ArangoDBConfigurations.TIMEOUT.get())
                .map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::timeout);

        settings.get(ArangoDBConfigurations.CHUCK_SIZE.get())
                .map(Object::toString).map(Integer::valueOf)
                .ifPresent(arangoDB::chunksize);

        settings.get(ArangoDBConfigurations.MAX_CONNECTIONS.get())
                .map(Object::toString).map(Integer::valueOf)
                .ifPresent(arangoDB::maxConnections);

        settings.get(ArangoDBConfigurations.USERSSL.get())
                .map(Object::toString).map(Boolean::valueOf)
                .ifPresent(arangoDB::useSsl);

        settings.get(ArangoDBConfigurations.HOST_LIST.get())
                .map(Object::toString).map(Boolean::valueOf)
                .ifPresent(arangoDB::acquireHostList);

        settings.get(ArangoDBConfigurations.LOADBALANCING.get()).map(Object::toString).map(LoadBalancingStrategy::valueOf)
                .ifPresent(arangoDB::loadBalancingStrategy);

        settings.get(ArangoDBConfigurations.PROTOCOL.get()).map(Object::toString).map(Protocol::valueOf)
                .ifPresent(arangoDB::useProtocol);

        settings.prefix(Arrays.asList(ArangoDBConfigurations.HOST.get(), Configurations.HOST.get()))
                .stream()
                .map(Object::toString)
                .map(ArangoDBHost::new)
                .flatMap(h -> h.getHost().stream())
                .forEach(h -> host(arangoDB, h));
    }


    private static void host(ArangoDBBuilder arangoDB, String host) {
        final String[] values = host.split(":");
        arangoDB.host(values[0], Integer.parseInt(values[1]));
    }

    private static class ArangoDBHost {
        private final String hots;

        private ArangoDBHost(String hots) {
            this.hots = hots;
        }

        public List<String> getHost() {
            return asList(this.hots.split(","));
        }
    }
}
