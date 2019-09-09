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
package org.eclipse.jnosql.diana.memcached.keyvalue;

import java.util.function.Supplier;

public enum MemcachedConfigurations implements Supplier<String> {

    DAEMON("memcached.daemon"),
    MAX_RECONNECT_DELAY("memcached.reconnect.delay"),
    PROTOCOL("memcached.protocol"),
    LOCATOR("memcached.locator"),
    AUTH_WAIT_TIME("memcached.auth.wait.time"),
    MAX_BLOCK_TIME("memcached.max.block.time"),
    TIMEOUT("memcached.timeout"),
    READ_BUFFER_SIZE("memcached.read.buffer.size"),
    SHOULD_OPTIMIZE("memcached.should.optimize"),
    TIMEOUT_THRESHOLD("memcached.timeout.threshold"),
    USE_NAGLE_ALGORITHM("memcached.nagle.algorithm"),
    USER("memcached.user"),
    PASSWORD("memcached.password"),
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
