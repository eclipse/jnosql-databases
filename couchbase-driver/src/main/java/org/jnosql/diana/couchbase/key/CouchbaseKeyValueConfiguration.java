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
import org.jnosql.diana.api.key.KeyValueConfiguration;
import org.jnosql.diana.couchbase.CouchbaseConfiguration;

import java.util.Objects;

/**
 * The couchbase implementation to {@link KeyValueConfiguration} that returns {@link CouchbaseBucketManagerFactory}.
 * It tries to read the diana-couchbase.properties file to get some informations:
 * <p>couchbase-host-: the prefix to add a new host</p>
 * <p>couchbase-user: the user</p>
 * <p>couchbase-password: the password</p>
 */
public class CouchbaseKeyValueConfiguration extends CouchbaseConfiguration
        implements KeyValueConfiguration<CouchbaseBucketManagerFactory> {


    /**
     * Creates a {@link CouchbaseBucketManagerFactory} from {@link CouchbaseEnvironment}
     *
     * @param environment the {@link CouchbaseEnvironment}
     * @return the new {@link CouchbaseBucketManagerFactory} instance
     * @throws NullPointerException when environment is null
     */
    public CouchbaseBucketManagerFactory getManagerFactory(CouchbaseEnvironment environment) throws NullPointerException {
        Objects.requireNonNull(environment, "environment is required");
        CouchbaseCluster couchbaseCluster = CouchbaseCluster.create(environment, nodes);
        return new CouchbaseBucketManagerFactory(couchbaseCluster, user, password);
    }

    @Override
    public CouchbaseBucketManagerFactory get() {
        return new CouchbaseBucketManagerFactory(CouchbaseCluster.create(nodes), user, password);
    }
}
