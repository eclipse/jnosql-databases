/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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
package org.eclipse.jnosql.communication.memcached.keyvalue;

import java.util.function.Supplier;

/**
 * An enumeration to show the available options to connect to the Memcached database.
 * It implements {@link Supplier}, where its it returns the property name that might be
 * overwritten by the system environment using Eclipse Microprofile or Jakarta Config API.
 *
 * @see jakarta.nosql.Settings
 */
public enum MemcachedConfigurations implements Supplier<String> {

    /**
     * The daemon state of the IO thread (defaults to true).
     */
    DAEMON("memcached.daemon"),
    /**
     * The maximum reconnect delay
     */
    MAX_RECONNECT_DELAY("memcached.reconnect.delay"),
    /**
     * The protocol type {@link  net.spy.memcached.ConnectionFactoryBuilder.Protocol}
     */
    PROTOCOL("memcached.protocol"),
    /**
     * The locator type {@link  net.spy.memcached.ConnectionFactoryBuilder.Locator}
     */
    LOCATOR("memcached.locator"),
    /**
     * Custom wait time for the authentication on connect/reconnect.
     */
    AUTH_WAIT_TIME("memcached.auth.wait.time"),
    /**
     * The maximum amount of time (in milliseconds) a client is willing to
     * wait for space to become available in an output queue.
     */
    MAX_BLOCK_TIME("memcached.max.block.time"),
    /**
     * The default operation timeout in milliseconds.
     */
    TIMEOUT("memcached.timeout"),
    /**
     * The read buffer size.
     */
    READ_BUFFER_SIZE("memcached.read.buffer.size"),
    /**
     * The default operation optimization is not desirable.
     */
    SHOULD_OPTIMIZE("memcached.should.optimize"),
    /**
     * The maximum timeout exception threshold.
     */
    TIMEOUT_THRESHOLD("memcached.timeout.threshold"),
    /**
     * Enable the Nagle algorithm.
     */
    USE_NAGLE_ALGORITHM("memcached.nagle.algorithm"),
    /**
     * The user's credential
     */
    USER("memcached.user"),
    /**
     * The password's credential.
     */
    PASSWORD("memcached.password"),
    /**
     * Database's host. It is a prefix to enumerate hosts. E.g.: memcached.host.1=localhost:11211
     */
    HOST("memcached.host");

    private final String configuration;

    MemcachedConfigurations(String configuration) {
        this.configuration = configuration;
    }

    @Override
    public String get() {
        return configuration;
    }
}
