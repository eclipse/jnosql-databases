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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import jakarta.nosql.document.DocumentCollectionManagerAsyncFactory;
import jakarta.nosql.document.DocumentCollectionManagerFactory;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;

/**
 * The OrientDB implementation of {@link DocumentCollectionManagerFactory}
 */
public class OrientDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory,
        DocumentCollectionManagerAsyncFactory{

    private final String host;
    private final String user;
    private final String password;
    private final ODatabaseType storageType;
    private final OrientDB orient;

    OrientDBDocumentCollectionManagerFactory(String host, String user, String password, String storageType) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.storageType = ofNullable(storageType)
                .map(String::toUpperCase)
                .map(ODatabaseType::valueOf)
                .orElse(ODatabaseType.PLOCAL);

        String prefix = this.storageType == ODatabaseType.MEMORY ? "embedded:" : "remote:";
        this.orient = new OrientDB(prefix + host, user, password, OrientDBConfig.defaultConfig());

    }

    @Override
    public OrientDBDocumentCollectionManager get(String database) {
        requireNonNull(database, "database is required");

        orient.createIfNotExists(database, storageType);
        ODatabasePool pool = new ODatabasePool(orient, database, user, password);
        return new DefaultOrientDBDocumentCollectionManager(pool);

    }

    @Override
    public OrientDBDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException,
            NullPointerException {
        requireNonNull(database, "database is required");
        orient.createIfNotExists(database, storageType);
        ODatabasePool pool = new ODatabasePool(orient, database, user, password);
        return new DefaultOrientDBDocumentCollectionManagerAsync(pool);
    }

    @Override
    public void close() {
        orient.close();
    }
}
