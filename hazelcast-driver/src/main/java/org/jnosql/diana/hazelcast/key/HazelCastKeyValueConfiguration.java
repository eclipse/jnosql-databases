/*
 * Copyright 2017 Eclipse Foundation
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
