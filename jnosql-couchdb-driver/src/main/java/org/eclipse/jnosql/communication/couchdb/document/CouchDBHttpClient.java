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
package org.eclipse.jnosql.communication.couchdb.document;

import jakarta.nosql.CommunicationException;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.apache.http.impl.client.CloseableHttpClient;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

final class CouchDBHttpClient {

    private final CloseableHttpClient client;

    private final String database;

    private final HttpExecute httpExecute;

    CouchDBHttpClient(CouchDBHttpConfiguration configuration, CloseableHttpClient client, String database) {
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

    public DocumentEntity update(DocumentEntity entity) {
        return this.httpExecute.update(database, entity);
    }

    public Stream<DocumentEntity> select(DocumentQuery query) {
        return this.httpExecute.select(database, query);
    }

    public void delete(DocumentDeleteQuery query) {
        this.httpExecute.delete(database, query);
    }

    public long count() {
        return httpExecute.count(database);
    }


    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new CommunicationException("An error when try to close the http client", e);
        }
    }

}
