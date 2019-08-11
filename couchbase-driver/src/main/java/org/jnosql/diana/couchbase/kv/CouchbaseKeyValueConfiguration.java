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
package org.jnosql.diana.couchbase.kv;


import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import jakarta.nosql.Settings;
import jakarta.nosql.kv.KeyValueConfiguration;
import org.jnosql.diana.couchbase.CouchbaseConfiguration;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation to {@link KeyValueConfiguration} that returns {@link DefaultCouchbaseBucketManagerFactory}.
 * It tries to read the diana-couchbase.properties file to get some informations:
 * <p>couchbase.host-: the prefix to add a new host</p>
 * <p>couchbase.user: the user</p>
 * <p>couchbase.password: the password</p>
 *
 * @see org.jnosql.diana.couchbase.CouchbaseConfigurations
 */
public class CouchbaseKeyValueConfiguration extends CouchbaseConfiguration
        implements KeyValueConfiguration {


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
        String user = Optional.ofNullable(getUser(settings)).orElse(this.user);
        String password = Optional.ofNullable(getPassword(settings)).orElse(this.password);
        List<String> hosts = getHosts(settings);
        return new DefaultCouchbaseBucketManagerFactory(CouchbaseCluster.create(hosts), user, password);
    }
}
