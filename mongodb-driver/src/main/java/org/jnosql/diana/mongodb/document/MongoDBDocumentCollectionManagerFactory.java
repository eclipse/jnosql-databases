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

package org.jnosql.diana.mongodb.document;

import com.mongodb.MongoClient;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsyncFactory;
import org.jnosql.diana.api.document.DocumentCollectionManagerFactory;

/**
 * The mongodb implementation to {@link DocumentCollectionManagerFactory}
 */
public class MongoDBDocumentCollectionManagerFactory implements DocumentCollectionManagerFactory<MongoDBDocumentCollectionManager>,
        DocumentCollectionManagerAsyncFactory<MongoDBDocumentCollectionManagerAsync> {

    private final MongoClient mongoClient;
    private final com.mongodb.async.client.MongoClient asyncMongoDatabase;

    MongoDBDocumentCollectionManagerFactory(MongoClient mongoClient,
                                            com.mongodb.async.client.MongoClient asyncMongoDatabase) {
        this.mongoClient = mongoClient;
        this.asyncMongoDatabase = asyncMongoDatabase;
    }

    @Override
    public MongoDBDocumentCollectionManager get(String database) {
        return new MongoDBDocumentCollectionManager(mongoClient.getDatabase(database));
    }

    @Override
    public MongoDBDocumentCollectionManagerAsync getAsync(String database) throws UnsupportedOperationException, NullPointerException {
        return new MongoDBDocumentCollectionManagerAsync(asyncMongoDatabase.getDatabase(database));
    }

    @Override
    public void close() {
        mongoClient.close();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MongoDBDocumentCollectionManagerFactory{");
        sb.append("mongoClient=").append(mongoClient);
        sb.append('}');
        return sb.toString();
    }
}
