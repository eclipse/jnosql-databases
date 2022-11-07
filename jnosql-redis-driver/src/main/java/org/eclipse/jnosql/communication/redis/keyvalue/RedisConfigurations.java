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
package org.eclipse.jnosql.communication.redis.keyvalue;

import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the Redis database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see jakarta.nosql.Settings
 */
public enum RedisConfigurations implements Supplier<String> {

    /**
     * The database host
     */
    HOST("redis.host"),
    /**
     * The database port
     */
    PORT("redis.port"),
    /**
     *
     */
    TIMEOUT("redis.timeout"),
    /**
     * The password's credential
     */
    PASSWORD("redis.password"),
    /**
     * The redis database's number
     */
    DATABASE("redis.database"),
    /**
     * The client's name
     */
    CLIENT_NAME("redis.client.name"),
    /**
     * The value for the maxTotal configuration attribute for pools created with this configuration instance.
     */
    MAX_TOTAL("redis.max.total"),
    /**
     * The value for the maxIdle configuration attribute for pools created with this configuration instance.
     */
    MAX_IDLE("redis.max.idle"),
    /**
     * Set the value for the minIdle configuration attribute for pools created with this configuration instance.
     */
    MIN_IDLE("redis.min.idle"),
    /**
     * Sets the value for the {@code maxWait} configuration attribute for pools created with this configuration instance.
     */
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
