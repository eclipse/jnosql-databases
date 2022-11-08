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


import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.keyvalue.KeyValueConfiguration;
import org.eclipse.jnosql.communication.driver.ConfigurationReader;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;

/**
 * The redis implementation of {@link KeyValueConfiguration} whose returns {@link RedisBucketManagerFactory}.
 * @see RedisConfigurations
 */
public final class RedisConfiguration implements KeyValueConfiguration {

    private static final String FILE_CONFIGURATION = "diana-redis.properties";
    private static final int DEFAULT_PORT = 6379;
    private static final int DEFAULT_TIMEOUT = 2000;
    private static final int DEFAULT_DATABASE = 0;
    private static final String DEFAULT_HOST = "localhost";
    private static final int DEFAULT_MAX_TOTAL = 1000;
    private static final int DEFAULT_MAX_IDLE = 10;
    private static final int DEFAULT_MIN_IDLE = 1;
    private static final int DEFAULT_MAX_WAIT_MILLIS = 3000;

    /**
     * Creates a {@link RedisConfiguration} from map configuration
     *
     * @param configurations the map configuration
     * @return the RedisConfiguration instance
     */
    public RedisBucketManagerFactory getManagerFactory(Map<String, String> configurations) {
        Objects.requireNonNull(configurations, "configurations is required");
        SettingsBuilder builder = Settings.builder();
        configurations.forEach((key, value) -> builder.put(key, value));
        return get(builder.build());
    }

    /**
     * Creates a {@link RedisBucketManagerFactory} instance from a {@link JedisPool}
     * @param jedisPool the jedis pool
     * @return a {@link RedisBucketManagerFactory} instance
     */
    public RedisBucketManagerFactory get(JedisPool jedisPool) {
        Objects.requireNonNull(jedisPool, "jedisPool is required");
        return new DefaultRedisBucketManagerFactory(jedisPool);
    }

    @Override
    public RedisBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return getManagerFactory(configuration);
    }

    @Override
    public RedisBucketManagerFactory get(Settings settings) {
        Objects.requireNonNull(settings, "settings is required");

        JedisPoolConfig poolConfig = getJedisPoolConfig(settings);
        JedisPool jedisPool = getJedisPool(settings, poolConfig);
        return new DefaultRedisBucketManagerFactory(jedisPool);
    }


    private JedisPool getJedisPool(Settings settings, JedisPoolConfig poolConfig) {

        String localhost = settings.get(asList(RedisConfigurations.HOST.get(), Configurations.HOST.get()))
                .map(Object::toString).orElse(DEFAULT_HOST);

        Integer port = settings.get(RedisConfigurations.PORT.get())
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_PORT);

        Integer timeout = settings.get(RedisConfigurations.TIMEOUT.get())
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_TIMEOUT);

        String password = settings.get(asList(RedisConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                .map(Object::toString).orElse(null);
        Integer database = settings.get(RedisConfigurations.DATABASE.get())
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_DATABASE);

        String clientName = settings.get(RedisConfigurations.CLIENT_NAME.get())
                .map(Object::toString).orElse(null);
        return new JedisPool(poolConfig, localhost, port, timeout, password, database, clientName);
    }

    private JedisPoolConfig getJedisPoolConfig(Settings settings) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();


        poolConfig.setMaxTotal(settings.get(RedisConfigurations.MAX_TOTAL.get())
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_MAX_TOTAL));

        poolConfig.setMaxIdle(settings.get(RedisConfigurations.MAX_IDLE.get())
                .map(Object::toString).map(Integer::parseInt).orElse(DEFAULT_MAX_IDLE));

        poolConfig.setMinIdle(   settings.get(RedisConfigurations.MIN_IDLE.get())
                .map(Object::toString).map(Integer::parseInt).orElse(DEFAULT_MIN_IDLE));

        poolConfig.setMaxWaitMillis(settings.get(RedisConfigurations.MAX_WAIT_MILLIS.get())
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_MAX_WAIT_MILLIS));
        return poolConfig;
    }

}
