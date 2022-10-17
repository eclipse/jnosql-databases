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


import com.couchbase.client.java.Cluster;
import jakarta.nosql.document.DocumentCollectionManagerFactory;

import java.util.Objects;

public class CouhbaseDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory{


    private final String host;
    private final String user;
    private final String password;
    private final Cluster cluster;
    CouhbaseDocumentCollectionManagerFactory(String host, String user, String password) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.cluster = Cluster.connect(host, user, password);
    }

    @Override
    public CouchbaseDocumentCollectionManager get(String database)  {
        Objects.requireNonNull(database, "database is required");
        return new DefaultCouchbaseDocumentCollectionManager(cluster, database);
    }


    @Override
    public void close() {
        cluster.close();
    }
}
