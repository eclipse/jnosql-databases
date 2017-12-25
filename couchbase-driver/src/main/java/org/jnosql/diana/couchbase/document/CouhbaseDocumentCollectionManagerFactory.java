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
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.query.N1qlQuery;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.util.List;
import java.util.Objects;

public class CouhbaseDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<CouchbaseDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<CouchbaseDocumentCollectionManagerAsync> {


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
        return new DefaultCouchbaseDocumentCollectionManagerAsync(get(database));
    }

    @Override
    public CouchbaseDocumentCollectionManager get(String database) throws UnsupportedOperationException, NullPointerException {
        Objects.requireNonNull(database, "database is required");
        CouchbaseCluster authenticate = couchbaseCluster.authenticate(user, password);
        ClusterManager clusterManager = authenticate.clusterManager();
        List<BucketSettings> buckets = clusterManager.getBuckets();
        if(buckets.stream().noneMatch(b -> b.name().equals(database))) {
            BucketSettings bucketSettings =  DefaultBucketSettings.builder().name(database).quota(120);
            clusterManager.insertBucket(bucketSettings);
            Bucket bucket = authenticate.openBucket(database);
            bucket.query(N1qlQuery.simple("CREATE PRIMARY INDEX index_" + database + " on " + database));
            bucket.close();

        }
        return new DefaultCouchbaseDocumentCollectionManager(authenticate.openBucket(database), database);
    }

    @Override
    public void close() {
        couchbaseCluster.disconnect();
    }
}
