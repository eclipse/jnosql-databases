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


import com.mongodb.Block;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.async.client.FindIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import jakarta.nosql.ExecuteAsyncQueryException;
import jakarta.nosql.Sort;
import jakarta.nosql.SortType;
import jakarta.nosql.document.DocumentCollectionManagerAsync;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jnosql.diana.document.Documents;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.mongodb.document.MongoDBUtils.ID_FIELD;
import static org.jnosql.diana.mongodb.document.MongoDBUtils.getDocument;

/**
 * The mongodb implementation of {@link DocumentCollectionManagerAsync} whose does not support the TTL methods:
 * <p>{@link MongoDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration)}</p>
 * <p>{@link MongoDBDocumentCollectionManagerAsync#insert(DocumentEntity, Duration, Consumer)}</p>
 */
public class MongoDBDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {


    private static final BsonDocument EMPTY = new BsonDocument();

    private final MongoDatabase asyncMongoDatabase;

    MongoDBDocumentCollectionManagerAsync(MongoDatabase asyncMongoDatabase) {
        this.asyncMongoDatabase = asyncMongoDatabase;
    }

    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        insert(entity, v -> {
        });
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }

    @Override
    public void insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::insert);
    }

    @Override
    public void insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        entities.forEach(e -> insert(e, ttl));
    }

    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        insert(entity, (aVoid, throwable) -> callBack.accept(entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }

    @Override
    public void update(DocumentEntity entity)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        requireNonNull(entity, "entity is required");
        update(entity, (d, throwable) -> {
        });
    }

    @Override
    public void update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        entities.forEach(this::update);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");
        update(entity, (d, throwable) -> callBack.accept(DocumentEntity.of(entity.getName(), Documents.of(d))));
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        requireNonNull(query, "query is required");
        delete(query, (deleteResult, throwable) -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        delete(query, (deleteResult, throwable) -> callBack.accept(null));

    }

    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        String collectionName = query.getDocumentCollection();
        MongoCollection<Document> collection = asyncMongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = query.getCondition().map(DocumentQueryConversor::convert).orElse(EMPTY);
        List<DocumentEntity> entities = new CopyOnWriteArrayList<>();
        FindIterable<Document> result = collection.find(mongoDBQuery);
        result.projection(Projections.include(query.getDocuments()));
        if (query.getSkip() > 0) {
            result.skip((int) query.getSkip());
        }

        if (query.getLimit() > 0) {
            result.limit((int) query.getLimit());
        }

        query.getSorts().stream().map(this::getSort).forEach(result::sort);
        Block<Document> documentBlock = d -> entities.add(createEntity(collectionName, d));
        SingleResultCallback<Void> voidSingleResultCallback = (v, e) -> callBack.accept(entities);
        result.forEach(documentBlock, voidSingleResultCallback);
    }

    @Override
    public void count(String documentCollection, Consumer<Long> callback) {
        requireNonNull(documentCollection, "documentCollection is required");
        requireNonNull(callback, "callback is required");
        MongoCollection<Document> collection = asyncMongoDatabase.getCollection(documentCollection);
        collection.count((l, e) -> callback.accept(l));
    }

    private DocumentEntity createEntity(String collectionName, Document document) {
        return DocumentEntity.of(collectionName, MongoDBUtils.of(document));
    }

    private void insert(DocumentEntity entity, SingleResultCallback<Void> callBack) {
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
                        "the field `_id` is required"));

        asyncCollection.findOneAndReplace(id, getDocument(entity), callBack);
    }

    private void delete(DocumentDeleteQuery query, SingleResultCallback<DeleteResult> callBack) {
        String collectionName = query.getDocumentCollection();
        com.mongodb.async.client.MongoCollection<Document> asyncCollection =
                asyncMongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = DocumentQueryConversor.convert(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("condition is required")));
        asyncCollection.deleteMany(mongoDBQuery, callBack);
    }


    @Override
    public void close() {

    }

    private Bson getSort(Sort sort) {
        boolean isAscending = SortType.ASC.equals(sort.getType());
        return isAscending?Sorts.ascending(sort.getName()): Sorts.descending(sort.getName());
    }

}
