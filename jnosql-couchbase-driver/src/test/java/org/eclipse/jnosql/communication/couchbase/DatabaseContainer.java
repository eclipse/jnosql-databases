package org.eclipse.jnosql.communication.couchbase;

import org.eclipse.jnosql.communication.couchbase.document.CouchbaseDocumentConfiguration;
import org.eclipse.jnosql.communication.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

public enum DatabaseContainer {

    INSTANCE;

    private CouchbaseContainer container;

    DatabaseContainer() {
        BucketDefinition bucketDefinition = new BucketDefinition(CouchbaseUtil.BUCKET_NAME)
                .withPrimaryIndex(true)
                .withReplicas(0)
                .withFlushEnabled(true);
        CouchbaseContainer container = new CouchbaseContainer("couchbase/server")
                .withBucket(bucketDefinition);
        container.start();
    }

    public CouchbaseDocumentConfiguration getDocumentConfiguration() {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
        configuration.setHost(container.getHost());
        configuration.setPassword(container.getPassword());
        configuration.setUser(container.getUsername());
        return configuration;
    }

    public CouchbaseKeyValueConfiguration getKeyValueConfiguration() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        configuration.setHost(container.getHost());
        configuration.setPassword(container.getPassword());
        configuration.setUser(container.getUsername());
        return configuration;
    }
}
