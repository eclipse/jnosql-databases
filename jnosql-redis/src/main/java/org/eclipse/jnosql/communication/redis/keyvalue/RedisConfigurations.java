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
 */
package org.eclipse.jnosql.communication.redis.keyvalue;



import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the Redis database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see org.eclipse.jnosql.communication.Settings
 */
public enum RedisConfigurations implements Supplier<String> {

    /**
     * The database host
     */
    HOST("jnosql.redis.host"),
    /**
     * The database port
     */
    PORT("jnosql.redis.port"),
    /**
     * The redis timeout, the default value 2000 on milliseconds
     */
    TIMEOUT("jnosql.redis.timeout"),
    /**
     * The password's credential
     */
    PASSWORD("jnosql.redis.password"),
    /**
     * The redis database number, the default value is 0
     */
    DATABASE("jnosql.redis.database"),
    /**
     * The client's name
     */
    CLIENT_NAME("jnosql.redis.client.name"),
    /**
     * The value for the maxTotal configuration attribute for pools created with this configuration instance.
     * The max number of thread to {@link redis.clients.jedis.JedisPoolConfig}, the default value 1000
     */
    MAX_TOTAL("jnosql.redis.max.total"),
    /**
     * The value for the maxIdle configuration attribute for pools created with this configuration instance.
     * The max idle {@link redis.clients.jedis.JedisPoolConfig}, the default value 10
     */
    MAX_IDLE("jnosql.redis.max.idle"),
    /**
     * The value for the minIdle configuration attribute for pools created with this configuration instance.
     * The min idle {@link redis.clients.jedis.JedisPoolConfig}, the default value 1
     */
    MIN_IDLE("jnosql.redis.min.idle"),
    /**
     * The value for the {@code maxWait} configuration attribute for pools created with this configuration instance.
     * The max wait on millis on {@link redis.clients.jedis.JedisPoolConfig}, the default value 3000
     */
    MAX_WAIT_MILLIS("jnosql.redis.max.wait.millis");

    private final String configuration;

    RedisConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
