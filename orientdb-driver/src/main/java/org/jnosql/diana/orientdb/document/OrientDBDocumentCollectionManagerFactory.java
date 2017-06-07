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


import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.io.IOException;

import static java.util.Optional.ofNullable;

/**
 * The OrientDB implementation of {@link DocumentCollectionManagerFactory}
 */
public class OrientDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<OrientDBDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<OrientDBDocumentCollectionManagerAsync> {

    private static final String DATABASE_TYPE = "document";
    private static final String STORAGE_TYPE = "plocal";

    private final String host;
    private final String user;
    private final String password;
    private final String storageType;

    OrientDBDocumentCollectionManagerFactory(String host, String user, String password, String storageType) {
        this.host = host;
        this.user = user;
        this.password = password;
        this.storageType = ofNullable(storageType).orElse(STORAGE_TYPE);
    }

    @Override
    public OrientDBDocumentCollectionManager get(String database) {
        try {
            OServerAdmin serverAdmin = new OServerAdmin(host)
                    .connect(user, password);

            if (!serverAdmin.existsDatabase(database, storageType)) {
                serverAdmin.createDatabase(database, DATABASE_TYPE, storageType);
            }
            OPartitionedDatabasePool pool = new OPartitionedDatabasePool("remote:" + host + '/' + database, user, password);
            return new OrientDBDocumentCollectionManager(pool);
        } catch (IOException e) {
            throw new OrientDBException("Error when getDocumentEntityManager", e);
        }
    }

    @Override
    public OrientDBDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        try {
            OServerAdmin serverAdmin = new OServerAdmin(host)
                    .connect(user, password);

            if (!serverAdmin.existsDatabase(database, storageType)) {
                serverAdmin.createDatabase(database, DATABASE_TYPE, storageType);
            }
            OPartitionedDatabasePool pool = new OPartitionedDatabasePool("remote:" + host + '/' + database, user, password);
            return new OrientDBDocumentCollectionManagerAsync(pool);
        } catch (IOException e) {
            throw new OrientDBException("Error when getDocumentEntityManager", e);
        }
    }

    @Override
    public void close() {

    }
}
