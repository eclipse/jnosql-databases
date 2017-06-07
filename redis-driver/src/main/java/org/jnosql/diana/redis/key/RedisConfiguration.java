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


import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * The redis implementation of {@link KeyValueConfiguration} whose returns {@link RedisKeyValueEntityManagerFactory}.
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
public final class RedisConfiguration implements KeyValueConfiguration<RedisKeyValueEntityManagerFactory> {

    private static final String REDIS_FILE_CONFIGURATION = "diana-redis.properties";

    private static final Logger LOGGER = Logger.getLogger(RedisConfiguration.class.getName());

    /**
     * Creates a {@link RedisConfiguration} from map configuration
     * @param configurations the map configuration
     * @return the RedisConfiguration instance
     */
    public RedisKeyValueEntityManagerFactory getManagerFactory(Map<String, String> configurations) {
        JedisPoolConfig poolConfig = getJedisPoolConfig(configurations);
        JedisPool jedisPool = getJedisPool(configurations, poolConfig);

        return new RedisKeyValueEntityManagerFactory(jedisPool);
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
    public RedisKeyValueEntityManagerFactory get() {
        try {
            Properties properties = new Properties();
            InputStream stream = RedisConfiguration.class.getClassLoader()
                    .getResourceAsStream(REDIS_FILE_CONFIGURATION);
            properties.load(stream);
            Map<String, String> collect = properties.keySet().stream()
                    .collect(Collectors.toMap(Object::toString, s -> properties.get(s).toString()));
            return getManagerFactory(collect);
        } catch (IOException e) {
            LOGGER.info("File does not found using default configuration");
            return getManagerFactory(Collections.emptyMap());
        }
    }
}
