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
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

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
        Bucket bucket = couchbaseCluster.authenticate(user, password).openBucket(database);
        return new DefaultCouchbaseDocumentCollectionManager(bucket, database);
    }

    @Override
    public void close() {
        couchbaseCluster.disconnect();
    }
}
