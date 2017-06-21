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


import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static org.jnosql.diana.mongodb.document.MongoDBUtils.getDocument;

/**
 * The mongodb implementation of {@link DocumentCollectionManagerAsync} whose does not support the TTL methods:
 * <p>{@link MongoDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration)}</p>
 * <p>{@link MongoDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration, Consumer)}</p>
 */
public class MongoDBDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {

    private static final String ID_FIELD = "_id";

    private final MongoDatabase asyncMongoDatabase;

    MongoDBDocumentCollectionManagerAsync(MongoDatabase asyncMongoDatabase) {
        this.asyncMongoDatabase = asyncMongoDatabase;
    }

    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, v -> {
        });
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, (aVoid, throwable) -> callBack.accept(entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }

    @Override
    public void update(DocumentEntity entity)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        update(entity, (d, throwable) -> {
        });
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        update(entity, (d, throwable) -> {
            callBack.accept(DocumentEntity.of(entity.getName(), Documents.of(d)));
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        delete(query, (deleteResult, throwable) -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, (deleteResult, throwable) -> callBack.accept(null));

    }

    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    private void save(DocumentEntity entity, SingleResultCallback<Void> callBack) {
        String collectionName = entity.getName();
        com.mongodb.async.client.MongoCollection<Document> collectionAsync =
                asyncMongoDatabase.getCollection(collectionName);
        Document document = getDocument(entity);
        collectionAsync.insertOne(document, callBack);
    }

    private void update(DocumentEntity entity, SingleResultCallback<Document> callBack) {
        String collectionName = entity.getName();
        com.mongodb.async.client.MongoCollection<Document> asyncCollection =
                asyncMongoDatabase.getCollection(collectionName);
        Document id = entity.find(ID_FIELD).map(d -> new Document(d.getName(), d.getValue().get()))
                .orElseThrow(() -> new UnsupportedOperationException("To update this DocumentEntity " +
                        "the field `id` is required"));

        asyncCollection.findOneAndReplace(id, getDocument(entity), callBack);
    }

    private void delete(DocumentDeleteQuery query, SingleResultCallback<DeleteResult> callBack) {
        String collectionName = query.getCollection();
        com.mongodb.async.client.MongoCollection<Document> asyncCollection =
                asyncMongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = DocumentQueryConversor.convert(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("condition is required")));
        asyncCollection.deleteMany(mongoDBQuery, callBack);
    }


    @Override
    public void close() {

    }

}
