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
package org.eclipse.jnosql.communication.couchbase.document;


import com.couchbase.client.java.CouchbaseCluster;
import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.eclipse.jnosql.communication.couchbase.util.CouchbaseClusterUtil;

public class CouhbaseDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory{


    private final String host;
    private final String user;
    private final String password;

    CouhbaseDocumentCollectionManagerFactory(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
    }

    @Override
    public CouchbaseDocumentCollectionManager get(String database) throws UnsupportedOperationException, NullPointerException {
        return new DefaultCouchbaseDocumentCollectionManager(authenticate.openBucket(database), database);
    }


    @Override
    public void close() {
    }
}
