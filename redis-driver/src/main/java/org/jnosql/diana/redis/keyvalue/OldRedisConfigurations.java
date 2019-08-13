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
package org.jnosql.diana.redis.keyvalue;

import java.util.function.Supplier;

/**
 * Use {@link RedisConfigurations}
 */
@Deprecated
public enum  OldRedisConfigurations implements Supplier<String> {

    HOST("redis-master-host"),
    PORT("redis-master-port"),
    TIMEOUT("redis-timeout"),
    PASSWORD("redis-password"),
    DATABASE("redis-database"),
    CLIENT_NAME("redis-clientName"),
    MAX_TOTAL("redis-configuration-max-total"),
    MAX_IDLE("redis-configuration-max-idle"),
    MIN_IDLE("redis-configuration-min-idle"),
    MAX_WAIT_MILLIS("redis-configuration-max--wait-millis");

    private final String configuration;

    OldRedisConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
