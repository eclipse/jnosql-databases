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
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

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
    private static final String DAEMON = "memcached.daemon";
    private static final String MAX_RECONNECT_DELAY = "memcached.reconnect.delay";

    private static final String PROTOCOL = "memcached.protocol";
    private static final String LOCATOR = "memcached.locator";
    private static final String AUTH_WAIT_TIME = "memcached.auth.wait.time";
    private static final String MAX_BLOCK_TIME = "memcached.max.block.time";
    private static final String TIMEOUT = "memcached.timeout";

    private static final String READ_BUFFER_SIZE = "memcached.read.buffer.size";
    private static final String SHOULD_OPTIMIZE = "memcached.should.optimize";
    private static final String TIMEOUT_THRESHOLD = "memcached.timeout.threshold";
    private static final String USE_NAGLE_ALGORITHM = "memcached.nagle.algorithm";


    @Override
    public MemcachedBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(Settings.of(new HashMap<>(configuration)));
    }

    @Override
    public MemcachedBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");
        ConnectionFactoryBuilder factoryBuilder = new ConnectionFactoryBuilder();

        ofNullable(settings.get(DAEMON)).map(Objects::toString)
                .map(Boolean::parseBoolean)
                .ifPresent(factoryBuilder::setDaemon);

        ofNullable(settings.get(MAX_RECONNECT_DELAY))
                .map(Objects::toString).map(Long::parseLong)
                .ifPresent(factoryBuilder::setMaxReconnectDelay);

        ofNullable(settings.get(PROTOCOL))
                .map(Objects::toString).map(Protocol::valueOf)
                .ifPresent(factoryBuilder::setProtocol);

        ofNullable(settings.get(LOCATOR))
                .map(Objects::toString).map(Locator::valueOf)
                .ifPresent(factoryBuilder::setLocatorType);

        ofNullable(settings.get(AUTH_WAIT_TIME))
                .map(Objects::toString).map(Long::parseLong)
                .ifPresent(factoryBuilder::setAuthWaitTime);

        ofNullable(settings.get(MAX_BLOCK_TIME))
                .map(Objects::toString).map(Long::parseLong)
                .ifPresent(factoryBuilder::setOpQueueMaxBlockTime);

        ofNullable(settings.get(TIMEOUT))
                .map(Objects::toString).map(Long::parseLong)
                .ifPresent(factoryBuilder::setOpTimeout);

        ofNullable(settings.get(READ_BUFFER_SIZE))
                .map(Objects::toString).map(Integer::parseInt)
                .ifPresent(factoryBuilder::setReadBufferSize);

        ofNullable(settings.get(SHOULD_OPTIMIZE))
                .map(Objects::toString).map(Boolean::parseBoolean)
                .ifPresent(factoryBuilder::setShouldOptimize);

        ofNullable(settings.get(TIMEOUT_THRESHOLD))
                .map(Objects::toString).map(Integer::parseInt)
                .ifPresent(factoryBuilder::setTimeoutExceptionThreshold);

        ofNullable(settings.get(USE_NAGLE_ALGORITHM))
                .map(Objects::toString).map(Boolean::parseBoolean)
                .ifPresent(factoryBuilder::setUseNagleAlgorithm);


        ConnectionFactory connectionFactory = factoryBuilder.build();
        return new MemcachedBucketManagerFactory(connectionFactory);
    }

}
