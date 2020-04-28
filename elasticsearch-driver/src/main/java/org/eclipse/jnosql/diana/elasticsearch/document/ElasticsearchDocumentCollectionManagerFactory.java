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
package org.eclipse.jnosql.diana.elasticsearch.document;


import jakarta.nosql.document.DocumentCollectionManagerFactory;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Objects;

import static java.lang.Boolean.TRUE;
import static java.nio.file.Files.readAllBytes;


/**
 * The elasticsearch implementation to {@link DocumentCollectionManagerFactory} that returns:
 * {@link ElasticsearchDocumentCollectionManager}
 * If the database does not exist, it tries to read a json mapping from the database name.
 * Eg: {@link ElasticsearchDocumentCollectionManagerFactory#get(String)} with database, if does not exist it tries to
 * read a "/database.json" file. The file must have the mapping to elasticsearch.
 */
public class ElasticsearchDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory{


    private final RestHighLevelClient client;

    ElasticsearchDocumentCollectionManagerFactory(RestHighLevelClient client) {
        this.client = client;
    }



    @Override
    public ElasticsearchDocumentCollectionManager get(String database) throws UnsupportedOperationException, NullPointerException {
        Objects.requireNonNull(database, "database is required");

        initDatabase(database);
        return new DefaultElasticsearchDocumentCollectionManager(client, database);
    }

    private byte[] getBytes(URL url) {
        try {
            return readAllBytes(Paths.get(url.toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new ElasticsearchException("An error when read the database mapping", e);
        }
    }

    private void initDatabase(String database) {
        boolean exists = isExists(database);
        if (!exists) {
            createIndex(database);
        }
    }

    private void createIndex(String database) {
        InputStream stream = ElasticsearchDocumentCollectionManagerFactory.class.getResourceAsStream('/' + database + ".json");
        if (Objects.nonNull(stream)) {
            try {
                RestClient lowLevelClient = client.getLowLevelClient();
                HttpEntity entity = new NStringEntity(getMappging(stream), ContentType.APPLICATION_JSON);
                Request request = new Request("PUT", database);
                request.addParameter("include_type_name", TRUE.toString());
                request.setEntity(entity);

                lowLevelClient.performRequest(request);
            } catch (Exception ex) {
                throw new ElasticsearchException("Error when create a new mapping", ex);
            }
        } else {
            try {
                CreateIndexRequest request = new CreateIndexRequest(database);
                client.indices().create(request, RequestOptions.DEFAULT);
            } catch (Exception ex) {
                throw new ElasticsearchException("Error when create a new mapping", ex);
            }
        }
    }

    private String getMappging(InputStream stream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        int nRead;
        byte[] data = new byte[1024];
        while ((nRead = stream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        buffer.flush();
        byte[] byteArray = buffer.toByteArray();

        return new String(byteArray, StandardCharsets.UTF_8);
    }

    private boolean isExists(String database) {
        try {
            client.indices().open(new OpenIndexRequest(database), RequestOptions.DEFAULT);
            return true;
        } catch (IOException e) {
            throw new ElasticsearchException("And error on admin access to verify if the database exists", e);
        } catch (ElasticsearchStatusException e) {
            return false;
        }
    }

    @Override
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new ElasticsearchException("An error when close the RestHighLevelClient client", e);
        }
    }
}
