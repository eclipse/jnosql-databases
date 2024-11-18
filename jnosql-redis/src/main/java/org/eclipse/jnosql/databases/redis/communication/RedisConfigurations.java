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
package org.eclipse.jnosql.databases.redis.communication;


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
     * The redis database number
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
    MAX_WAIT_MILLIS("jnosql.redis.max.wait.millis"),
    /**
     * The value for the connection timeout in milliseconds configuration attribute for the jedis client configuration
     * created with this configuration instance.
     * The connection timeout on millis on {@link redis.clients.jedis.JedisClientConfig}, the default value 2000
     */
    CONNECTION_TIMEOUT("jnosql.redis.connection.timeout"),
    /**
     * The value for the socket timeout in milliseconds configuration attribute for the jedis client configuration with
     * this configuration instance.
     * The socket timeout on millis on {@link redis.clients.jedis.JedisClientConfig}, the default value 2000
     */
    SOCKET_TIMEOUT("jnosql.redis.socket.timeout"),
    /**
     * The value for the user configuration attribute for the jedis client configuration with this configuration instance.
     * The user on {@link redis.clients.jedis.JedisClientConfig}
     */
    USER("jnosql.redis.user"),
    /**
     * The value for the ssl configuration attribute for the jedis client configuration with this configuration instance.
     * The ssl on {@link redis.clients.jedis.JedisClientConfig}
     */
    SSL("jnosql.redis.ssl"),
    /**
     * The value for the protocol configuration attribute for the jedis client configuration with this configuration instance.
     * The protocol on {@link redis.clients.jedis.JedisClientConfig}
     */
    REDIS_PROTOCOL("jnosql.redis.protocol"),
    /**
     * The value for the clientset info disabled configuration attribute for the jedis client configuration with this configuration instance.
     * The clientset info disabled on {@link redis.clients.jedis.JedisClientConfig}
     */
    CLIENTSET_INFO_CONFIG_DISABLED("jnosql.redis.clientset.info.config.disabled"),
    /**
     * The value for the clientset info configuration libname suffix attribute for the jedis client configuration with this configuration instance.
     * The clientset info libname suffix on {@link redis.clients.jedis.JedisClientConfig}
     */
    CLIENTSET_INFO_CONFIG_LIBNAME_SUFFIX("jnosql.redis.clientset.info.config.libname.suffix"),
    /**
     * The value for the sentinel basic configuration attribute for the jedis client configuration with this configuration instance.
     */
    SENTINEL("jnosql.redis.sentinel"),
    /**
     * The value for the cluster configuration attribute for the jedis client configuration with this configuration instance.
     */
    CLUSTER("jnosql.redis.cluster");

    private final String configuration;

    RedisConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }

    public static enum SingleRedisConfigurationsResolver
            implements RedisConfigurationsResolver {

        INSTANCE;

        @Override
        public Supplier<String> connectionTimeoutSupplier() {
            return RedisConfigurations.CONNECTION_TIMEOUT;
        }

        @Override
        public Supplier<String> socketTimeoutSupplier() {
            return RedisConfigurations.SOCKET_TIMEOUT;
        }

        @Override
        public Supplier<String> clientNameSupplier() {
            return RedisConfigurations.CLIENT_NAME;
        }

        @Override
        public Supplier<String> userSupplier() {
            return RedisConfigurations.USER;
        }

        @Override
        public Supplier<String> passwordSupplier() {
            return RedisConfigurations.PASSWORD;
        }

        @Override
        public Supplier<String> timeoutSupplier() {
            return RedisConfigurations.TIMEOUT;
        }

        @Override
        public Supplier<String> sslSupplier() {
            return RedisConfigurations.SSL;
        }

        @Override
        public Supplier<String> redisProtocolSupplier() {
            return RedisConfigurations.REDIS_PROTOCOL;
        }

        @Override
        public Supplier<String> clientsetInfoConfigLibNameSuffixSupplier() {
            return RedisConfigurations.CLIENTSET_INFO_CONFIG_LIBNAME_SUFFIX;
        }

        @Override
        public Supplier<String> clientsetInfoConfigDisabled() {
            return RedisConfigurations.CLIENTSET_INFO_CONFIG_DISABLED;
        }
    }
}
