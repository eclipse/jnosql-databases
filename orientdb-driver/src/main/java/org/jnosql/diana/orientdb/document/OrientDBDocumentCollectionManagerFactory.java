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
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.io.IOException;
import java.util.Objects;

import static java.util.Optional.ofNullable;

/**
 * The OrientDB implementation of {@link DocumentCollectionManagerFactory}
 */
public class OrientDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<OrientDBDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<OrientDBDocumentCollectionManagerAsync> {

    private static final String DATABASE_TYPE = "document";

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
                .map(String::toString)
                .map(ODatabaseType::valueOf)
                .orElse(ODatabaseType.PLOCAL);
        this.orient = new OrientDB("remote:" + host, OrientDBConfig.defaultConfig());

    }

    @Override
    public OrientDBDocumentCollectionManager get(String database) {
        Objects.requireNonNull(database, "database is required");
        try {

            orient.createIfNotExists(database, storageType);
            ODatabaseSession session = orient.open(database, user, password);

            return new DefaultOrientDBDocumentCollectionManager(session);
        } catch (IOException e) {
            throw new OrientDBException("Error when getDocumentEntityManager", e);
        }
    }

    @Override
    public OrientDBDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        Objects.requireNonNull(database, "database is required");
        try {
            OServerAdmin serverAdmin = new OServerAdmin(host)
                    .connect(user, password);

            if (!serverAdmin.existsDatabase(database, storageType)) {
                serverAdmin.createDatabase(database, DATABASE_TYPE, storageType);
            }
            OPartitionedDatabasePool pool = new OPartitionedDatabasePool("remote:" + host + '/' + database, user, password);
            return new DefaultOrientDBDocumentCollectionManagerAsync(pool);
        } catch (IOException e) {
            throw new OrientDBException("Error when getDocumentEntityManager", e);
        }
    }

    @Override
    public void close() {
        orient.close();
    }
}
