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
package org.jnosql.diana.redis.key;

import java.util.function.Supplier;

public enum RedisConfigurations implements Supplier<String> {

    HOST("redis.host"),
    PORT("redis.port"),
    TIMEOUT("redis.timeout"),
    PASSWORD("redis.password"),
    DATABASE("redis.database"),
    CLIENT_NAME("redis.clientName"),
    MAX_TOTAL("redis.max.total"),
    MAX_IDLE("redis.max.idle"),
    MIN_IDLE("redis.min.idle"),
    MAX_WAIT_MILLIS("redis.max.wait.millis");

    private final String configuration;

    RedisConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
