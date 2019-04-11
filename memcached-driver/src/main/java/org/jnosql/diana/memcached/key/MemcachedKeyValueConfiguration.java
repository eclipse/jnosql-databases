/*
 *  Copyright (c) 2019 Otávio Santana and others
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
package org.jnosql.diana.memcached.key;


import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The memcached implementation of {@link KeyValueConfiguration} that returns
 * {@link MemcachedBucketManagerFactory}. It tries to read the diana-memcached.properties file
 * that has the properties:
 * <p>memcached.instanceName: the instance name</p>
 * <p>memcached.host-: as prefix to n host where n is the number of host, eg: memcached-host-1: host </p>
 *
 */
public class MemcachedKeyValueConfiguration implements KeyValueConfiguration<MemcachedBucketManagerFactory> {

    private static final String FILE_CONFIGURATION = "diana-memcached.properties";


    @Override
    public MemcachedBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(Settings.of(new HashMap<>(configuration)));
    }

    @Override
    public MemcachedBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");
        ConnectionFactoryBuilder factoryBuilder = new ConnectionFactoryBuilder();

        ConnectionFactory connectionFactory = factoryBuilder.build();
        return new MemcachedBucketManagerFactory(connectionFactory);
    }

}
