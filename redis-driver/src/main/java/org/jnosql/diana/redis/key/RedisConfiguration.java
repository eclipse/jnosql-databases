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

package org.jnosql.diana.redis.key;


import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.Settings.SettingsBuilder;
import jakarta.nosql.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.Objects;

import static java.util.Arrays.asList;

/**
 * The redis implementation of {@link KeyValueConfiguration} whose returns {@link RedisBucketManagerFactory}.
 * It tries to read diana-redis.properties file.
 * <p>redis.host: the host client </p>
 * <p>redis.port: the port, the default value 6379</p>
 * <p>redis.timeout: the redis timeout, the default value 2000 on milis</p>
 * <p>redis.password: the password</p>
 * <p>redis.database: the redis database number, the default value is 0</p>
 * <p>redis.clientName: the redis client name</p>
 * <p>redis.max.total: The max number of thread to {@link JedisPoolConfig}, the default value 1000 </p>
 * <p>redis.max.idle: The max idle {@link JedisPoolConfig}, the default value 10 </p>
 * <p>redis.min.idle: The min idle {@link JedisPoolConfig}, the default value 1 </p>
 * <p>redis.max.wait.millis: The max wait on millis on {@link JedisPoolConfig}, the default value 3000 </p>
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
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
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

        String localhost = settings.get(asList(OldRedisConfigurations.HOST.get(),
                RedisConfigurations.HOST.get(), Configurations.HOST.get()))
                .map(Object::toString).orElse(DEFAULT_HOST);

        Integer port = settings.get(asList(OldRedisConfigurations.PORT.get(),
                RedisConfigurations.PORT.get()))
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_PORT);

        Integer timeout = settings.get(asList(OldRedisConfigurations.TIMEOUT.get(),
                RedisConfigurations.TIMEOUT.get()))
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_TIMEOUT);

        String password = settings.get(asList(OldRedisConfigurations.PASSWORD.get(),
                RedisConfigurations.PASSWORD.get(), Configurations.PASSWORD.get()))
                .map(Object::toString).orElse(null);
        Integer database = settings.get(asList(OldRedisConfigurations.DATABASE.get(),
                RedisConfigurations.DATABASE.get()))
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_DATABASE);

        String clientName = settings.get(asList(OldRedisConfigurations.CLIENT_NAME.get(),
                RedisConfigurations.CLIENT_NAME.get()))
                .map(Object::toString).orElse(null);
        return new JedisPool(poolConfig, localhost, port, timeout, password, database, clientName);
    }

    private JedisPoolConfig getJedisPoolConfig(Settings settings) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();


        poolConfig.setMaxTotal(settings.get(asList(OldRedisConfigurations.MAX_TOTAL.get(),
                RedisConfigurations.MAX_TOTAL.get())).map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_MAX_TOTAL));

        poolConfig.setMaxIdle(settings.get(asList(OldRedisConfigurations.MAX_IDLE.get(),
                RedisConfigurations.MAX_IDLE.get()))
                .map(Object::toString).map(Integer::parseInt).orElse(DEFAULT_MAX_IDLE));

        poolConfig.setMinIdle(   settings.get(asList(OldRedisConfigurations.MIN_IDLE.get(),
                RedisConfigurations.MIN_IDLE.get()))
                .map(Object::toString).map(Integer::parseInt).orElse(DEFAULT_MIN_IDLE));

        poolConfig.setMaxWaitMillis(settings.get(asList(OldRedisConfigurations.MAX_WAIT_MILLIS.get(),
                RedisConfigurations.MAX_WAIT_MILLIS.get()))
                .map(Object::toString).map(Integer::parseInt)
                .orElse(DEFAULT_MAX_WAIT_MILLIS));
        return poolConfig;
    }


}
