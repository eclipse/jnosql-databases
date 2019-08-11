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


import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.ConnectionFactoryBuilder.Locator;
import net.spy.memcached.ConnectionFactoryBuilder.Protocol;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.auth.AuthDescriptor;
import jakarta.nosql.Configurations;
import jakarta.nosql.Settings;
import jakarta.nosql.kv.KeyValueConfiguration;
import org.jnosql.diana.driver.ConfigurationReader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * The memcached implementation of {@link KeyValueConfiguration} that returns
 * {@link MemcachedBucketManagerFactory}. It tries to read the diana-memcached.properties file
 * that has the properties:
 * <p>memcached.daemon: {@link ConnectionFactoryBuilder#setDaemon(boolean)}</p>
 * <p>memcached.reconnect.delay: {@link ConnectionFactoryBuilder#setMaxReconnectDelay(long)}</p>
 * <p>memcached.protocol: {@link ConnectionFactoryBuilder#setProtocol(Protocol)}</p>
 * <p>memcached.locator: {@link ConnectionFactoryBuilder#setLocatorType(Locator)}</p>
 * <p>memcached.auth.wait.time: {@link ConnectionFactoryBuilder#setAuthWaitTime(long)}</p>
 * <p>memcached.max.block.time: {@link ConnectionFactoryBuilder#setOpQueueMaxBlockTime(long)}</p>
 * <p>memcached.timeout: {@link ConnectionFactoryBuilder#setOpTimeout(long)}</p>
 * <p>memcached.read.buffer.size: {@link ConnectionFactoryBuilder#setReadBufferSize(int)}</p>
 * <p>memcached.should.optimize: {@link ConnectionFactoryBuilder#setShouldOptimize(boolean)}</p>
 * <p>memcached.timeout.threshold: {@link ConnectionFactoryBuilder#setTimeoutExceptionThreshold(int)}</p>
 * <p>memcached.nagle.algorithm: {@link ConnectionFactoryBuilder#setUseNagleAlgorithm(boolean)}</p>
 * <p>memcached.user: the user</p>
 * <p>memcached.password: the password</p>
 * <p>memcached.host.: define the host to connect defined to n hots, e.g.: memcached.host.1: localhost:11211</p>
 *
 * @see MemcachedConfigurations
 */
public class MemcachedKeyValueConfiguration implements KeyValueConfiguration {

    public static final String FILE_CONFIGURATION = "diana-memcached.properties";

    @Override
    public MemcachedBucketManagerFactory get() {
        Map<String, String> configuration = ConfigurationReader.from(FILE_CONFIGURATION);
        return get(Settings.of(new HashMap<>(configuration)));
    }

    @Override
    public MemcachedBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");
        ConnectionFactoryBuilder factoryBuilder = new ConnectionFactoryBuilder();

        settings.get(MemcachedConfigurations.DAEMON.get(), Boolean.class)
                .ifPresent(factoryBuilder::setDaemon);

        settings.get(MemcachedConfigurations.MAX_RECONNECT_DELAY.get(), Long.class)
                .ifPresent(factoryBuilder::setMaxReconnectDelay);

        settings.get(MemcachedConfigurations.PROTOCOL.get(), Protocol.class)
                .ifPresent(factoryBuilder::setProtocol);

        settings.get(MemcachedConfigurations.LOCATOR.get(), Locator.class)
                .ifPresent(factoryBuilder::setLocatorType);

        settings.get(MemcachedConfigurations.AUTH_WAIT_TIME.get(), Long.class)
                .ifPresent(factoryBuilder::setAuthWaitTime);

        settings.get(MemcachedConfigurations.MAX_BLOCK_TIME.get(), Long.class)
                .ifPresent(factoryBuilder::setOpQueueMaxBlockTime);

        settings.get(MemcachedConfigurations.TIMEOUT.get(), Long.class)
                .ifPresent(factoryBuilder::setOpTimeout);

        settings.get(MemcachedConfigurations.READ_BUFFER_SIZE.get(), Integer.class)
                .ifPresent(factoryBuilder::setReadBufferSize);

        settings.get(MemcachedConfigurations.SHOULD_OPTIMIZE.get(), Boolean.class)
                .ifPresent(factoryBuilder::setShouldOptimize);

        settings.get(MemcachedConfigurations.TIMEOUT_THRESHOLD.get(), Integer.class)
                .ifPresent(factoryBuilder::setTimeoutExceptionThreshold);

        settings.get(MemcachedConfigurations.USE_NAGLE_ALGORITHM.get(), Boolean.class)
                .ifPresent(factoryBuilder::setUseNagleAlgorithm);

        settings.get(asList(MemcachedConfigurations.USER.get(), Configurations.USER.get()))
                .map(Object::toString)
                .ifPresent(u -> {
                    String password = ofNullable(settings.get(asList(MemcachedConfigurations.PASSWORD.get()
                            , Configurations.PASSWORD.get())))
                            .map(Object::toString).orElse(null);
                    factoryBuilder.setAuthDescriptor(AuthDescriptor.typical(u, password));
                });


        List<String> hots = settings.prefix(asList(MemcachedConfigurations.HOST.get(), Configurations.HOST.get()))
                .stream()
                .map(Object::toString)
                .collect(Collectors.toList());

        List<InetSocketAddress> addresses = hots.isEmpty() ? Collections.emptyList() : AddrUtil.getAddresses(hots);
        ConnectionFactory connectionFactory = factoryBuilder.build();

        try {
            return new MemcachedBucketManagerFactory(new MemcachedClient(connectionFactory, addresses));
        } catch (IOException e) {
            throw new MemcachedException("There is an error when try to create da BucketManager", e);
        }
    }

    /**
     * Creates a {@link MemcachedBucketManagerFactory} from a {@link MemcachedClient}
     *
     * @param client the client
     * @return a {@link MemcachedBucketManagerFactory} instance
     */
    public MemcachedBucketManagerFactory get(MemcachedClient client) {
        Objects.requireNonNull(client, "client is required");
        return new MemcachedBucketManagerFactory(client);
    }
}

