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


import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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
public final class RedisConfiguration implements KeyValueConfiguration<RedisBucketManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-redis.properties";
    public static final String HOST = "redis-master-host";
    public static final String PORT = "redis-master-port";
    public static final String TIMEOUT = "redis-timeout";
    public static final String PASSWORD = "redis-password";
    public static final String DATABASE = "redis-database";
    public static final String CLIENT_NAME = "redis-clientName";
    public static final String MAX_TOTAL = "redis-configuration-max-total";
    public static final String MAX_IDLE = "redis-configuration-max-idle";
    public static final String MIN_IDLE = "redis-configuration-min-idle";
    public static final String MAX_WAIT_MILLIS = "redis-configuration-max--wait-millis";

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

    private JedisPool getJedisPool(Settings settings, JedisPoolConfig poolConfig) {

        String localhost = settings.get(HOST).map(Object::toString).orElse("localhost");
        Integer port = settings.get(PORT).map(Object::toString).map(Integer::parseInt).orElse(6379);
        Integer timeout = settings.get(TIMEOUT).map(Object::toString).map(Integer::parseInt).orElse(2000);
        String password = settings.get(PASSWORD).map(Object::toString).orElse(null);
        Integer database = settings.get(DATABASE).map(Object::toString).map(Integer::parseInt).orElse(0);
        ;
        String clientName = settings.get(CLIENT_NAME).map(Object::toString).orElse(null);
        return new JedisPool(poolConfig, localhost, port, timeout, password, database, clientName);
    }

    private JedisPoolConfig getJedisPoolConfig(Settings settings) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();


        poolConfig.setMaxTotal(settings.get(MAX_TOTAL).map(Object::toString).map(Integer::parseInt).orElse(1000));
        poolConfig.setMaxIdle(settings.get(MAX_IDLE).map(Object::toString).map(Integer::parseInt).orElse(10));
        poolConfig.setMinIdle(settings.get(MIN_IDLE).map(Object::toString).map(Integer::parseInt).orElse(1));
        poolConfig.setMaxWaitMillis(settings.get(MAX_WAIT_MILLIS).map(Object::toString).map(Integer::parseInt)
                .orElse(3000));
        return poolConfig;
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
}
