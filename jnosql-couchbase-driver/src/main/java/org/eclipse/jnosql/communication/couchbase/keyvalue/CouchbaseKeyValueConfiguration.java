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
package org.eclipse.jnosql.communication.couchbase.keyvalue;


import jakarta.nosql.Settings;
import jakarta.nosql.keyvalue.KeyValueConfiguration;
import org.eclipse.jnosql.communication.couchbase.CouchbaseConfiguration;
import org.eclipse.jnosql.communication.couchbase.CouchbaseConfigurations;

import static java.util.Objects.requireNonNull;

/**
 * The couchbase implementation to {@link KeyValueConfiguration} that returns {@link DefaultCouchbaseBucketManagerFactory}.
 * It tries to read the diana-couchbase.properties file to get some informations:
 * <p>couchbase.host: to identify the connection</p>
 * <p>couchbase.user: the user</p>
 * <p>couchbase.password: the password</p>
 *
 * @see CouchbaseConfigurations
 */
public class CouchbaseKeyValueConfiguration extends CouchbaseConfiguration
        implements KeyValueConfiguration {


    @Override
    public CouchbaseBucketManagerFactory get() {
        return new DefaultCouchbaseBucketManagerFactory(toCouchbaseSettings());
    }

    @Override
    public CouchbaseBucketManagerFactory get(Settings settings) {
        requireNonNull(settings, "settings is required");
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        configuration.update(settings);
        return new DefaultCouchbaseBucketManagerFactory(configuration.toCouchbaseSettings());
    }
}
