/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.jnosql.diana.arangodb;

import java.util.function.Supplier;

/**
 * This class has all the configurations on the arangoDB
 */
public enum ArangoDBConfigurations implements Supplier<String> {

    HOST("arangodb.host"),
    USER("arangodb.user"),
    PASSWORD("arangodb.password"),
    PORT("arangodb.port"),
    TIMEOUT("arangodb.timeout"),
    CHUCK_SIZE("arangodb.chuckSize"),
    USERSSL("arangodb.userSsl"),
    LOADBALANCING("arangodb.loadBalancingStrategy"),
    HOSTS("arangodb.hosts"),
    PROTOCOL("arangodb.protocol"),
    CHUNKSIZE("arangodb.chunksize"),
    CONNECTIONS_MAX("arangodb.connections.max");


    private final String configuration;

    ArangoDBConfigurations(String configuration) {
        this.configuration = configuration;
    }


    @Override
    public String get() {
        return configuration;
    }
}
