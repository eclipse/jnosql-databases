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

import java.io.IOException;

final class CouchDBHttpClient {

    private final CouchDBHttpConfiguration configuration;

    private final CloseableHttpClient client;


    CouchDBHttpClient(CouchDBHttpConfiguration configuration, CloseableHttpClient client) {
        this.configuration = configuration;
        this.client = client;
    }

    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new JNoSQLException("An error when try to close the http client", e);
        }
    }
}
