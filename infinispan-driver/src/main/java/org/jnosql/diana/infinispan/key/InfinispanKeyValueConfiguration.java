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
 *   The Infinispan Team
 */

package org.jnosql.diana.infinispan.key;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

/**
 * The Infinispan implementation of {@link KeyValueConfiguration} that returns
 * {@link InfinispanKeyValueEntityManagerFactory}. It tries to read the diana-infinispan.properties file
 * that has the properties:
 * <p>infinispan-config: the optional path to an Infinispan configuration file</p>
 * <p>infinispan-host-: as prefix to n host where n is the number of host, eg: infinispan-host-1: host </p>
 *
 */
public class InfinispanKeyValueConfiguration implements KeyValueConfiguration<InfinispanKeyValueEntityManagerFactory> {

    private static final String INFINISPAN_FILE_CONFIGURATION = "diana-infinispan.properties";

    /**
     * Creates a {@link InfinispanKeyValueEntityManagerFactory} from configuration map
     * @param configurations the configuration map
     * @return the InfinispanKeyValueEntityManagerFactory instance
     * @throws NullPointerException when configurations is null
     */
    public InfinispanKeyValueEntityManagerFactory get(Map<String, String> configurations) {
        List<String> servers = configurations.keySet().stream().filter(s -> s.startsWith("infinispan-server-"))
              .collect(Collectors.toList());
        if (!servers.isEmpty()) {
            org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
            for(String server : servers) {
                builder.addServer().host(server);
            }
            return  new InfinispanKeyValueEntityManagerFactory(new RemoteCacheManager(builder.build()));
        } else if (configurations.containsKey("infinispan-config")) {
            try {
                return new InfinispanKeyValueEntityManagerFactory(new DefaultCacheManager(configurations.get("infinispan-config")));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
            builder.globalJmxStatistics().allowDuplicateDomains(true);
            return new InfinispanKeyValueEntityManagerFactory(new DefaultCacheManager(builder.build()));
        }
    }

    /**
     * Creates a {@link InfinispanKeyValueEntityManagerFactory} from infinispan config
     * @param config the {@link org.infinispan.configuration.cache.Configuration}
     * @return the InfinispanKeyValueEntityManagerFactory instance
     * @throws NullPointerException when config is null
     */
    public InfinispanKeyValueEntityManagerFactory get(org.infinispan.configuration.cache.Configuration config)throws NullPointerException {
        requireNonNull(config, "config is required");

        return new InfinispanKeyValueEntityManagerFactory(new DefaultCacheManager(config));
    }

    @Override
    public InfinispanKeyValueEntityManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(INFINISPAN_FILE_CONFIGURATION);
        return get(configuration);
    }

    @Override
    public InfinispanKeyValueEntityManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");

        Map<String, String> configurations = new HashMap<>();
        settings.entrySet().forEach(e -> configurations.put(e.getKey(), e.getValue().toString()));
        return get(configurations);
    }
}
