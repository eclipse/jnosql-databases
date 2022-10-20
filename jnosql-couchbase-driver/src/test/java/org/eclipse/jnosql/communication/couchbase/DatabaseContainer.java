/*
 *  Copyright (c) 2022 Otávio Santana and others
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

import com.couchbase.client.core.error.BucketNotFoundException;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import com.couchbase.client.java.manager.collection.CollectionManager;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.collection.ScopeSpec;
import com.couchbase.client.java.manager.query.AsyncQueryIndexManager;
import com.couchbase.client.java.manager.query.CreatePrimaryQueryIndexOptions;
import com.couchbase.client.java.manager.query.GetAllQueryIndexesOptions;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.query.QueryIndexManager;
import org.eclipse.jnosql.communication.couchbase.document.CouchbaseDocumentConfiguration;
import org.eclipse.jnosql.communication.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public enum DatabaseContainer {

    INSTANCE;

    private CouchbaseContainer container;

    DatabaseContainer() {
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
