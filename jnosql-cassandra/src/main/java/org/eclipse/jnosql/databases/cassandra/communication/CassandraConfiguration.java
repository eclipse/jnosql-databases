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

package org.eclipse.jnosql.databases.cassandra.communication;


import org.eclipse.jnosql.communication.Settings;
import org.eclipse.jnosql.communication.column.ColumnConfiguration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * The Cassandra implementation to {@link ColumnConfiguration}  that returns
 * {@link CassandraColumnManagerFactory}
 *
 * @see CassandraConfigurations
 */
public final class CassandraConfiguration implements ColumnConfiguration {


    private CassandraColumnManagerFactory getManagerFactory(Map<String, String> configurations) {
        requireNonNull(configurations);
        CassandraProperties properties = CassandraProperties.of(configurations);
        ExecutorService executorService = properties.createExecutorService();
        return new CassandraColumnManagerFactory(properties.createCluster(), properties.getQueries(), executorService);
    }


    @Override
    public CassandraColumnManagerFactory apply(Settings settings) throws NullPointerException {
        requireNonNull(settings, "settings is required");
        Map<String, String> configurations = new HashMap<>();

        List<String> keys = settings.keySet()
                .stream()
                .filter(k -> k.startsWith("jnosql."))
                .collect(Collectors.toUnmodifiableList());

        for (String key : keys) {
            settings.get(key, String.class).ifPresent(v -> configurations.put(key, v));
        }
        return getManagerFactory(configurations);
    }

}
