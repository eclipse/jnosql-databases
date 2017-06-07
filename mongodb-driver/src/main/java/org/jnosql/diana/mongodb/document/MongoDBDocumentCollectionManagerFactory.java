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
