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
package org.eclipse.jnosql.communication.couchbase;

import org.eclipse.jnosql.communication.couchbase.document.CouchbaseDocumentConfiguration;
import org.eclipse.jnosql.communication.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public enum Database {

    INSTANCE;

    private CouchbaseContainer container;

    Database() {
        //TODO create couchbase container to run all tests instead of run an outside container
        BucketDefinition bucketDefinition = new BucketDefinition(CouchbaseUtil.BUCKET_NAME)
                .withPrimaryIndex(true)
                .withReplicas(0)
                .withFlushEnabled(true);
        container = new CouchbaseContainer("couchbase/server")
                .withBucket(bucketDefinition)
                .withExposedPorts(8091, 8092, 8093, 8094, 11207, 11210, 11211, 18091, 18092, 18093);
        container.start();
        CouchbaseDocumentConfiguration configuration = setup(new CouchbaseDocumentConfiguration());
        CouchbaseSettings settings = configuration.toCouchbaseSettings();
        settings.setUp("jnosql");
    }

    public CouchbaseDocumentConfiguration getDocumentConfiguration() {
        CouchbaseDocumentConfiguration configuration = setup(new CouchbaseDocumentConfiguration());
        return configuration;
    }

    public CouchbaseKeyValueConfiguration getKeyValueConfiguration() {
        CouchbaseKeyValueConfiguration configuration = setup(new CouchbaseKeyValueConfiguration());
        return configuration;
    }

    private <T extends CouchbaseConfiguration> T setup(T configuration) {
        configuration.setHost(container.getConnectionString());
        configuration.setUser(container.getUsername());
        configuration.setPassword(container.getPassword());
        return configuration;
    }
}
