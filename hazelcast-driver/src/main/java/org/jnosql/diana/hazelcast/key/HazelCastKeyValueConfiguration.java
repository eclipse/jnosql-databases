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

package org.jnosql.diana.hazelcast.key;


import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The hazelcast implementation of {@link KeyValueConfiguration} that returns
 * {@link HazelCastKeyValueEntityManagerFactory}. It tries to read the diana-hazelcast.properties file
 * that has the properties:
 * <p>hazelcast-instanceName: the instance name</p>
 * <p>hazelcast-host-: as prefix to n host where n is the number of host, eg: hazelcast-host-1: host </p>
 *
 */
public class HazelCastKeyValueConfiguration implements KeyValueConfiguration<HazelCastKeyValueEntityManagerFactory> {

    private static final String HAZELCAST_FILE_CONFIGURATION = "diana-hazelcast.properties";


    /**
     * Creates a {@link HazelCastKeyValueEntityManagerFactory} from configuration map
     * @param configurations the configuration map
     * @return the HazelCastKeyValueEntityManagerFactory instance
     * @throws NullPointerException when configurations is null
     */
    public HazelCastKeyValueEntityManagerFactory get(Map<String, String> configurations) throws NullPointerException {

        List<String> servers = configurations.keySet().stream().filter(s -> s.startsWith("hazelcast-hoster-"))
                .collect(Collectors.toList());
        Config config = new Config(configurations.getOrDefault("hazelcast-instanceName", "hazelcast-instanceName"));

        HazelcastInstance hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
        return new HazelCastKeyValueEntityManagerFactory(hazelcastInstance);
    }

    /**
     * Creates a {@link HazelCastKeyValueEntityManagerFactory} from hazelcast config
     * @param config the {@link Config}
     * @return the HazelCastKeyValueEntityManagerFactory instance
     * @throws NullPointerException when config is null
     */
    public HazelCastKeyValueEntityManagerFactory get(Config config)throws NullPointerException {
        Objects.requireNonNull(config, "config is required");
        HazelcastInstance hazelcastInstance = Hazelcast.getOrCreateHazelcastInstance(config);
        return new HazelCastKeyValueEntityManagerFactory(hazelcastInstance);
    }

    @Override
    public HazelCastKeyValueEntityManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(HAZELCAST_FILE_CONFIGURATION);
        return get(configuration);
    }
}
