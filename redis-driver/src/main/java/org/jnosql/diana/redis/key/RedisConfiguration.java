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
 * <p>redis-master-host: the host client </p>
 * <p>redis-master-port: the port, the default value 6379</p>
 * <p>redis-timeout: the redis timeout, the default value 2000 on milis</p>
 * <p>redis-password: the password</p>
 * <p>redis-database: the redis database number, the default value is 0</p>
 * <p>redis-clientName: the redis client name</p>
 * <p>redis-configuration-max-total: The max number of thread to {@link JedisPoolConfig}, the default value 1000 </p>
 * <p>redis-configuration-max-idle: The max idle {@link JedisPoolConfig}, the default value 10 </p>
 * <p>redis-configuration-min-idle: The min idle {@link JedisPoolConfig}, the default value 1 </p>
 * <p>redis-configuration-max--wait-millis: The max wait on millis on {@link JedisPoolConfig}, the default value 3000 </p>
 */
public final class RedisConfiguration implements KeyValueConfiguration<RedisBucketManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-redis.properties";

    /**
     * Creates a {@link RedisConfiguration} from map configuration
     *
     * @param configurations the map configuration
     * @return the RedisConfiguration instance
     */
    public RedisBucketManagerFactory getManagerFactory(Map<String, String> configurations) {
        JedisPoolConfig poolConfig = getJedisPoolConfig(configurations);
        JedisPool jedisPool = getJedisPool(configurations, poolConfig);

        return new DefaultRedisBucketManagerFactory(jedisPool);
    }

    private JedisPool getJedisPool(Map<String, String> configurations, JedisPoolConfig poolConfig) {
        String localhost = configurations.getOrDefault("redis-master-host", "localhost");
        Integer port = Integer.valueOf(configurations.getOrDefault("redis-master-port", "6379"));
        Integer timeout = Integer.valueOf(configurations.getOrDefault("redis-timeout", "2000"));
        String password = configurations.getOrDefault("redis-password", null);
        Integer database = Integer.valueOf(configurations.getOrDefault("redis-database", "0"));
        String clientName = configurations.getOrDefault("redis-clientName", null);
        return new JedisPool(poolConfig, localhost, port, timeout, password, database, clientName);
    }

    private JedisPoolConfig getJedisPoolConfig(Map<String, String> configurations) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(Integer.valueOf(configurations.getOrDefault("redis-configuration-max-total", "1000")));
        poolConfig.setMaxIdle(Integer.valueOf(configurations.getOrDefault("redis-configuration-max-idle", "10")));
        poolConfig.setMinIdle(Integer.valueOf(configurations.getOrDefault("redis-configuration-min-idle", "1")));
        poolConfig.setMaxWaitMillis(Integer.valueOf(configurations
                .getOrDefault("redis-configuration-max--wait-millis", "3000")));
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
        Map<String, String> configurations = new HashMap<>();
        settings.forEach((key, value) -> configurations.put(key, value.toString()));
        return getManagerFactory(configurations);
    }
}
