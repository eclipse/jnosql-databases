/*
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

import com.mongodb.async.SingleResultCallback;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.time.Duration;
import java.util.List;
import java.util.function.Consumer;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;

public class MongoDBDocumentCollectionManager implements DocumentCollectionManager {

    private final MongoDatabase mongoDatabase;

    private final com.mongodb.async.client.MongoDatabase asyncMongoDatabase;

    private final ValueWriter writerField = ValueWriterDecorator.getInstance();


    MongoDBDocumentCollectionManager(MongoDatabase mongoDatabase,
                                     com.mongodb.async.client.MongoDatabase asyncMongoDatabase) {
        this.mongoDatabase = mongoDatabase;
        this.asyncMongoDatabase = asyncMongoDatabase;
    }


    @Override
    public DocumentEntity save(DocumentEntity entity) {
        String collectionName = entity.getName();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document document = getDocument(entity);
        collection.insertOne(document);
        boolean hasNotId = entity.getDocuments().stream()
                .map(document1 -> document1.getName()).noneMatch(k -> k.equals("_id"));
        if (hasNotId) {
            entity.add(Documents.of("_id", document.get("_id")));
        }
        return entity;
    }


    @Override
    public void saveAsync(DocumentEntity entity) {
        saveAsync(entity, (aVoid, throwable) -> {
        });
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support save with TTL");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        saveAsync(entity, (aVoid, throwable) -> callBack.accept(entity));
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) {
        throw new UnsupportedOperationException("MongoDB does not support saveAsync with TTL");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        DocumentEntity copy = entity.copy();
        String collectionName = entity.getName();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document id = copy.find("_id")
                .map(d -> new Document(d.getName(), d.getValue().get()))
                .orElseThrow(() -> new UnsupportedOperationException("To update this DocumentEntity " +
                        "the field `id` is required"));
        copy.remove("_id");
        collection.findOneAndReplace(id, getDocument(entity));
        return entity;
    }

    @Override
    public void updateAsync(DocumentEntity entity)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        updateAsync(entity, (d, throwable) -> {
        });
    }

    @Override
    public void updateAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        updateAsync(entity, (d, throwable) -> {
            callBack.accept(DocumentEntity.of(entity.getName(), Documents.of(d)));
        });
    }


    @Override
    public void delete(DocumentQuery query) {
        String collectionName = query.getCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        List<Bson> collect = query.getConditions().stream().map(DocumentQueryConversor::convert).collect(toList());
        DeleteResult deleteResult = collection.deleteMany(Filters.and(collect));
        System.out.println(deleteResult);
    }

    @Override
    public void deleteAsync(DocumentQuery query) {
        deleteAsync(query, (deleteResult, throwable) -> {
        });
    }

    @Override
    public void deleteAsync(DocumentQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        deleteAsync(query, (deleteResult, throwable) -> callBack.accept(null));

    }


    @Override
    public List<DocumentEntity> find(DocumentQuery query) {
        String collectionName = query.getCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        List<Bson> collect = query.getConditions().stream().map(DocumentQueryConversor::convert).collect(toList());
        if (collect.isEmpty()) {
            return stream(collection.find().spliterator(), false).map(Documents::of)
                    .map(ds -> DocumentEntity.of(collectionName, ds)).collect(toList());
        }
        FindIterable<Document> documents = collection.find(Filters.and(collect));
        return stream(documents.spliterator(), false).map(Documents::of)
                .map(ds -> DocumentEntity.of(collectionName, ds)).collect(toList());

    }

    @Override
    public void findAsync(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void close() {

    }

    private Document getDocument(DocumentEntity entity) {
        Document document = new Document();
        entity.getDocuments().stream().forEach(d -> document.append(d.getName(), convert(d.getValue())));
        return document;
    }

    private Object convert(Value value) {
        Object val = value.get();
        if (val instanceof org.jnosql.diana.api.document.Document) {
            org.jnosql.diana.api.document.Document subDocument = (org.jnosql.diana.api.document.Document) val;
            Object converted = convert(subDocument.getValue());
            return new Document(subDocument.getName(), converted);
        }
        if (writerField.isCompatible(val.getClass())) {
            return writerField.write(val);
        }
        return val;
    }

    private void saveAsync(DocumentEntity entity, SingleResultCallback<Void> callBack) {
        String collectionName = entity.getName();
        com.mongodb.async.client.MongoCollection<Document> collectionAsync =
                asyncMongoDatabase.getCollection(collectionName);
        Document document = getDocument(entity);
        collectionAsync.insertOne(document, callBack);
    }

    private void updateAsync(DocumentEntity entity, SingleResultCallback<Document> callBack) {
        String collectionName = entity.getName();
        com.mongodb.async.client.MongoCollection<Document> asyncCollection =
                asyncMongoDatabase.getCollection(collectionName);
        Document id = entity.find("_id").map(d -> new Document(d.getName(), d.getValue().get()))
                .orElseThrow(() -> new UnsupportedOperationException("To update this DocumentEntity " +
                        "the field `id` is required"));

        asyncCollection.findOneAndReplace(id, getDocument(entity), callBack);
    }

    private void deleteAsync(DocumentQuery query, SingleResultCallback<DeleteResult> callBack) {
        String collectionName = query.getCollection();
        com.mongodb.async.client.MongoCollection<Document> asyncCollection =
                asyncMongoDatabase.getCollection(collectionName);
        List<Bson> collect = query.getConditions().stream().map(DocumentQueryConversor::convert).collect(toList());
        asyncCollection.deleteMany(Filters.and(collect), callBack);
    }

}
