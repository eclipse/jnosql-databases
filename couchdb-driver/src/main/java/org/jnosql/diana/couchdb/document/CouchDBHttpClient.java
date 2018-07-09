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
package org.jnosql.diana.couchdb.document;

import org.apache.http.impl.client.CloseableHttpClient;
import org.jnosql.diana.api.JNoSQLException;
import org.jnosql.diana.api.document.DocumentEntity;

import java.io.IOException;
import java.util.List;

final class CouchDBHttpClient {



    private final CouchDBHttpConfiguration configuration;

    private final CloseableHttpClient client;

    private final String database;

    private final HttpExecute httpExecute;

    CouchDBHttpClient(CouchDBHttpConfiguration configuration, CloseableHttpClient client, String database) {
        this.configuration = configuration;
        this.client = client;
        this.database = database;
        this.httpExecute = new HttpExecute(configuration, client);
    }

    void createDatabase() {
        List<String> databases = httpExecute.getDatabases();
        if (!databases.contains(database)) {
            httpExecute.createDatabase(database);
        }
        return;
    }

    public DocumentEntity insert(DocumentEntity entity) {
        return this.httpExecute.insert(database, entity);
    }




    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new JNoSQLException("An error when try to close the http client", e);
        }
    }
}
