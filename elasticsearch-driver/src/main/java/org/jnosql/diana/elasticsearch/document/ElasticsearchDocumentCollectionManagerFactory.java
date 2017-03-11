/*
 * Copyright 2017 Otavio Santana and others
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jnosql.diana.elasticsearch.document;


import org.elasticsearch.client.Client;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import static java.nio.file.Files.readAllBytes;


/**
 * The elasticsearch implementation to {@link DocumentCollectionManagerFactory} that returns:
 * {@link ElasticsearchDocumentCollectionManager} and {@link ElasticsearchDocumentCollectionManagerAsync}.
 * If the database does not exist, it tries to read a json mapping from the database name.
 * Eg: {@link ElasticsearchDocumentCollectionManagerFactory#get(String)} with database, if does not exist it tries to
 * read a "/database.json" file. The file must have the mapping to elasticsearch.
 */
public class ElasticsearchDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<ElasticsearchDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<ElasticsearchDocumentCollectionManagerAsync> {


    private final Client client;

    ElasticsearchDocumentCollectionManagerFactory(Client client) {
        this.client = client;
    }

    @Override
    public ElasticsearchDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        initDatabase(database);
        return new ElasticsearchDocumentCollectionManagerAsync(client, database);
    }


    @Override
    public ElasticsearchDocumentCollectionManager get(String database) throws UnsupportedOperationException, NullPointerException {
        Objects.requireNonNull(database, "database is required");

        initDatabase(database);
        return new ElasticsearchDocumentCollectionManager(client, database);
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
            URL url = ElasticsearchDocumentCollectionManagerFactory.class.getResource('/' + database + ".json");
            if (Objects.nonNull(url)) {
                byte[] bytes = getBytes(url);
                client.admin().indices().prepareCreate(database).setSource(bytes).get();
            }
        }
    }

    private boolean isExists(String database) {
        try {
            return client.admin().indices().prepareExists(database).execute().get().isExists();
        } catch (InterruptedException | ExecutionException e) {
            throw new ElasticsearchException("And error on admin access to verify if the database exists", e);
        }
    }

    @Override
    public void close() {
        client.close();
    }
}
