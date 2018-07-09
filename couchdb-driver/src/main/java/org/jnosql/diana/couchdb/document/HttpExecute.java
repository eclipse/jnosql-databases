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
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.jnosql.diana.driver.JsonbSupplier;

import javax.json.bind.Jsonb;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HttpExecute {

    private static final Jsonb JSONB = JsonbSupplier.getInstance().get();

    public static final Type LIST_STRING = new ArrayList<String>() {
    }.getClass().getGenericSuperclass();

    public static final Type JSON = new HashMap<String, Object>() {
    }.getClass().getGenericSuperclass();

    private final CouchDBHttpConfiguration configuration;

    private final CloseableHttpClient client;

    HttpExecute(CouchDBHttpConfiguration configuration, CloseableHttpClient client) {
        this.configuration = configuration;
        this.client = client;
    }

    public List<String> getDatabases() {
        HttpGet httpget = new HttpGet(configuration.getUrl().concat("_all_dbs"));
        try (CloseableHttpResponse result = client.execute(httpget)) {
            if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new CouchDBHttpClientException("There is an error when load the database status");
            }
            HttpEntity entity = result.getEntity();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            entity.writeTo(stream);
            return JSONB.fromJson(new String(stream.toByteArray()), LIST_STRING);
        } catch (Exception ex) {
            throw new CouchDBHttpClientException("An error when load the databases", ex);
        }
    }

    private void createDatabase(String database) {
        HttpPut httpPut = new HttpPut(configuration.getUrl().concat(database));

        try (CloseableHttpResponse result = client.execute(httpPut)) {
            if (result.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                throw new CouchDBHttpClientException("There is an error when load the database status");
            }
            HttpEntity entity = result.getEntity();
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            entity.writeTo(stream);
            Map<String, Object> json = JSONB.fromJson(new String(stream.toByteArray()), JSON);
        } catch (Exception ex) {
            throw new CouchDBHttpClientException("An error when load the databases", ex);
        }
    }
}
