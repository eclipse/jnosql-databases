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
package org.jnosql.diana.couchbase.document;


import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.ClusterManager;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.util.Objects;

public class CouhbaseDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<CouchbaseDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<CouchbaseDocumentCollectionManagerAsync> {

    private static final String DEFAULT_BUCKET = "default";


    private final CouchbaseCluster couchbaseCluster;
    private final String user;
    private final String password;

    CouhbaseDocumentCollectionManagerFactory(CouchbaseCluster couchbaseCluster, String user, String password) {
        this.couchbaseCluster = couchbaseCluster;
        this.user = user;
        this.password = password;
    }

    @Override
    public CouchbaseDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        return new CouchbaseDocumentCollectionManagerAsync(get(database));
    }

    @Override
    public CouchbaseDocumentCollectionManager get(String database) throws UnsupportedOperationException, NullPointerException {
        Objects.requireNonNull(database, "database is required");
        ClusterManager clusterManager = couchbaseCluster.clusterManager(user, password);

  /*

        if(!clusterManager.hasBucket(bucketName)){
            BucketSettings settings = DefaultBucketSettings.builder().name(bucketName);
            clusterManager.insertBucket(settings);
        }*/
        if (DEFAULT_BUCKET.equals(database)) {
            Bucket bucket = couchbaseCluster.openBucket(database);
            bucket.bucketManager().createN1qlPrimaryIndex(true, false);
            return new CouchbaseDocumentCollectionManager(bucket, database);
        }
        return new CouchbaseDocumentCollectionManager(couchbaseCluster.openBucket(database, password), database);
    }

    @Override
    public void close() {
        couchbaseCluster.disconnect();
    }
}
