/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
