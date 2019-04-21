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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;

import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.jnosql.diana.api.Configurations;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.SettingsBuilder;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

/**
 * The Infinispan implementation of {@link KeyValueConfiguration} that returns
 * {@link InfinispanBucketManagerFactory}. It tries to read the diana-infinispan.properties file
 * that has the properties:
 * <p>infinispan.config: the optional path to an Infinispan configuration file</p>
 * <p>infinispan.host-: as prefix to n host where n is the number of host, eg: infinispan-host-1: host </p>
 *
 */
public class InfinispanKeyValueConfiguration implements KeyValueConfiguration<InfinispanBucketManagerFactory> {

    private static final String INFINISPAN_FILE_CONFIGURATION = "diana-infinispan.properties";

    /**
     * Creates a {@link InfinispanBucketManagerFactory} from configuration map
     * @param configurations the configuration map
     * @return the InfinispanBucketManagerFactory instance
     * @throws NullPointerException when configurations is null
     */
    public InfinispanBucketManagerFactory get(Map<String, String> configurations) {
        requireNonNull(configurations, "configurations is required");
        SettingsBuilder builder = Settings.builder();
        configurations.entrySet().forEach(e -> builder.put(e.getKey(), e.getValue()));
        return get(builder.build());
    }

    /**
     * Creates a {@link InfinispanBucketManagerFactory} from infinispan config
     * @param config the {@link org.infinispan.configuration.cache.Configuration}
     * @return the InfinispanBucketManagerFactory instance
     * @throws NullPointerException when config is null
     */
    public InfinispanBucketManagerFactory get(org.infinispan.configuration.cache.Configuration config)throws NullPointerException {
        requireNonNull(config, "config is required");

        return new InfinispanBucketManagerFactory(new DefaultCacheManager(config));
    }

    @Override
    public InfinispanBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(INFINISPAN_FILE_CONFIGURATION);
        return get(configuration);
    }

    @Override
    public InfinispanBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");

        List<String> servers = settings.prefix(Arrays.asList(OldInfinispanConfigurations.HOST.get(),
                InfinispanConfigurations.HOST.get(), Configurations.HOST.get()))
                .stream().map(Object::toString).collect(Collectors.toList());

        Optional<String> config = settings.get(Arrays.asList(OldInfinispanConfigurations.CONFIG.get(),
                InfinispanConfigurations.CONFIG.get()))
                .map(Object::toString);
        if (!servers.isEmpty()) {
            org.infinispan.client.hotrod.configuration.ConfigurationBuilder builder = new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
            for(String server : servers) {
                builder.addServer().host(server);
            }
            return  new InfinispanBucketManagerFactory(new RemoteCacheManager(builder.build()));
        } else if (config.isPresent()) {
            try {
                return new InfinispanBucketManagerFactory(new DefaultCacheManager(config.get()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
            builder.globalJmxStatistics().allowDuplicateDomains(true);
            return new InfinispanBucketManagerFactory(new DefaultCacheManager(builder.build()));
        }
    }
}
