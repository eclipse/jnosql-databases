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
package org.jnosql.diana.couchbase.key;


import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import org.jnosql.diana.api.Settings;
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.couchbase.CouchbaseConfiguration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation to {@link KeyValueConfiguration} that returns {@link DefaultCouchbaseBucketManagerFactory}.
 * It tries to read the diana-couchbase.properties file to get some informations:
 * <p>couchbase-host-: the prefix to add a new host</p>
 * <p>couchbase-user: the user</p>
 * <p>couchbase-password: the password</p>
 */
public class CouchbaseKeyValueConfiguration extends CouchbaseConfiguration
        implements KeyValueConfiguration<CouchbaseBucketManagerFactory> {


    /**
     * Creates a {@link DefaultCouchbaseBucketManagerFactory} from {@link CouchbaseEnvironment}
     *
     * @param environment the {@link CouchbaseEnvironment}
     * @return the new {@link DefaultCouchbaseBucketManagerFactory} instance
     * @throws NullPointerException when environment is null
     */
    public CouchbaseBucketManagerFactory getManagerFactory(CouchbaseEnvironment environment) throws NullPointerException {
        Objects.requireNonNull(environment, "environment is required");
        CouchbaseCluster couchbaseCluster = CouchbaseCluster.create(environment, nodes);
        return new DefaultCouchbaseBucketManagerFactory(couchbaseCluster, user, password);
    }

    @Override
    public CouchbaseBucketManagerFactory get() {
        return new DefaultCouchbaseBucketManagerFactory(CouchbaseCluster.create(nodes), user, password);
    }

    @Override
    public CouchbaseBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");

        Map<String, String> configurations = new HashMap<>();
        settings.forEach((key, value) -> configurations.put(key, value.toString()));

        List<String> hosts = new ArrayList<>();

        configurations.keySet()
                .stream()
                .filter(k -> k.startsWith(COUCHBASE_HOST))
                .sorted()
                .map(configurations::get)
                .forEach(this::add);

        String user = configurations.get(COUCHBASE_USER);
        String password = configurations.get(COUCHBASE_PASSWORD);
        return new DefaultCouchbaseBucketManagerFactory(CouchbaseCluster.create(hosts), user, password);
    }
}
