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

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jnosql.diana.api.JNoSQLException;
import org.jnosql.diana.driver.JsonbSupplier;

import javax.json.bind.Jsonb;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

final class CouchDBHttpClient {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();
    public static final Type LIST_STRING = new ArrayList<String>() {
    }.getClass().getGenericSuperclass();

    private final CouchDBHttpConfiguration configuration;

    private final CloseableHttpClient client;

    private final String database;


    CouchDBHttpClient(CouchDBHttpConfiguration configuration, CloseableHttpClient client, String database) {
        this.configuration = configuration;
        this.client = client;
        this.database = database;
    }

    void createDatabase() {
        HttpGet httpget = new HttpGet(Commands.ALL_DBS.getUrl(configuration.getUrl()));
        try (CloseableHttpResponse result = client.execute(httpget)) {
            if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new CouchDBHttpClientException("There is an error when load the database status");
            }
            HttpEntity entity = result.getEntity();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            entity.writeTo(stream);
            List<String> databases = JSONB.fromJson(new String(stream.toByteArray()), LIST_STRING);
            if (databases.contains(database)) {
                return;
            }
            System.out.println(database);
        } catch (Exception ex) {
            throw new CouchDBHttpClientException("An error when load the databases", ex);
        }
    }

    public void close() {
        try {
            this.client.close();
        } catch (IOException e) {
            throw new JNoSQLException("An error when try to close the http client", e);
        }
    }
}
