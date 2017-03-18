/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
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
