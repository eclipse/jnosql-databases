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

import com.arangodb.entity.LoadBalancingStrategy;
import org.jnosql.diana.api.Settings;

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

    private ArangoDBBuilders() {
    }

    public static void feed(Settings settings, ArangoDBBuilder arangoDB) {

        ofNullable(settings.get(HOST)).map(Object::toString).ifPresent(arangoDB::host);
        ofNullable(settings.get(USER)).map(Object::toString).ifPresent(arangoDB::user);
        ofNullable(settings.get(PASSWORD)).map(Object::toString).ifPresent(arangoDB::password);

        ofNullable(settings.get(PORT)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get(CHUCK_SIZE)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::port);
        ofNullable(settings.get(TIMEOUT)).map(Object::toString).map(Integer::valueOf).ifPresent(arangoDB::timeout);
        ofNullable(settings.get(USER_SSL)).map(Object::toString).map(Boolean::valueOf).ifPresent(arangoDB::useSsl);
        ofNullable(settings.get(LOAD_BALANCING_STRATEGY)).map(Object::toString).map(LoadBalancingStrategy::valueOf)
                .ifPresent(arangoDB::loadBalancingStrategy);
    }
}
