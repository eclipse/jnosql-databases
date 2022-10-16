package org.eclipse.jnosql.communication.couchbase;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.manager.bucket.BucketManager;
import com.couchbase.client.java.manager.bucket.BucketSettings;
import com.couchbase.client.java.manager.bucket.BucketType;
import org.eclipse.jnosql.communication.couchbase.document.CouchbaseDocumentConfiguration;
import org.eclipse.jnosql.communication.couchbase.keyvalue.CouchbaseKeyValueConfiguration;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.util.Map;

public enum DatabaseContainer {

    INSTANCE;

    private CouchbaseContainer container;

    DatabaseContainer() {
     /*   BucketDefinition bucketDefinition = new BucketDefinition(CouchbaseUtil.BUCKET_NAME)
                .withPrimaryIndex(true)
                .withReplicas(0)
                .withFlushEnabled(true);
        container = new CouchbaseContainer("couchbase/server")
                .withBucket(bucketDefinition)
                .withExposedPorts(8091, 8092, 8093, 8094, 11207, 11210, 11211, 18091, 18092, 18093);
        container.start();*/
    }

    public CouchbaseDocumentConfiguration getDocumentConfiguration() {
        CouchbaseDocumentConfiguration configuration = new CouchbaseDocumentConfiguration();
//        configuration.setHost(container.getHost());
//        configuration.setPassword(container.getPassword());
//        configuration.setUser(container.getUsername());
        configuration.setHost("couchbase://localhost");
        configuration.setUser("root");
        configuration.setPassword("123456");
        return configuration;
    }

    public CouchbaseKeyValueConfiguration getKeyValueConfiguration() {
        CouchbaseKeyValueConfiguration configuration = new CouchbaseKeyValueConfiguration();
        configuration.setHost("couchbase://localhost");
        configuration.setUser("root");
        configuration.setPassword("123456");
        return configuration;
    }
}
