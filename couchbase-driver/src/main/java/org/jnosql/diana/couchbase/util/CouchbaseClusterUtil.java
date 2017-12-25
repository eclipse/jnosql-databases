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
package org.jnosql.diana.couchbase.util;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.query.N1qlQuery;

import java.util.List;
import java.util.Objects;

public final class CouchbaseClusterUtil {
    private CouchbaseClusterUtil() {
    }

    public static CouchbaseCluster getCouchbaseCluster(String database, CouchbaseCluster couchbaseCluster, String user, String password) {
        Objects.requireNonNull(database, "database is required");
        CouchbaseCluster authenticate = couchbaseCluster.authenticate(user, password);
        ClusterManager clusterManager = authenticate.clusterManager();
        List<BucketSettings> buckets = clusterManager.getBuckets();
        if(buckets.stream().noneMatch(b -> b.name().equals(database))) {
            BucketSettings bucketSettings =  DefaultBucketSettings.builder().name(database).quota(100);
            clusterManager.insertBucket(bucketSettings);
            Bucket bucket = authenticate.openBucket(database);
            bucket.query(N1qlQuery.simple("CREATE PRIMARY INDEX index_" + database + " on " + database));
            bucket.close();

        }
        return authenticate;
    }

}
