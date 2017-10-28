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

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;

import java.time.Duration;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.mongodb.document.MongoDBUtils.getDocument;

/**
 * The mongodb implementation to {@link DocumentCollectionManager} that does not support TTL methods
 * <p>{@link MongoDBDocumentCollectionManager#insert(DocumentEntity, Duration)}</p>
 */
public class MongoDBDocumentCollectionManager implements DocumentCollectionManager {

    private static final String ID_FIELD = "_id";
    private static final BsonDocument EMPTY = new BsonDocument();

    private final MongoDatabase mongoDatabase;


    MongoDBDocumentCollectionManager(MongoDatabase mongoDatabase) {
        this.mongoDatabase = mongoDatabase;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        String collectionName = entity.getName();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document document = getDocument(entity);
        collection.insertOne(document);
        boolean hasNotId = entity.getDocuments().stream()
                .map(document1 -> document1.getName()).noneMatch(k -> k.equals(ID_FIELD));
        if (hasNotId) {
            entity.add(Documents.of(ID_FIELD, document.get(ID_FIELD)));
        }
        return entity;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("MongoDB does not support save with TTL");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        DocumentEntity copy = entity.copy();
        String collectionName = entity.getName();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document id = copy.find(ID_FIELD)
                .map(d -> new Document(d.getName(), d.getValue().get()))
                .orElseThrow(() -> new UnsupportedOperationException("To update this DocumentEntity " +
                        "the field `id` is required"));
        copy.remove(ID_FIELD);
        collection.findOneAndReplace(id, getDocument(entity));
        return entity;
    }


    @Override
    public void delete(DocumentDeleteQuery query) {
        String collectionName = query.getDocumentCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = DocumentQueryConversor.convert(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("condition is required")));
        DeleteResult deleteResult = collection.deleteMany(mongoDBQuery);
    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        String collectionName = query.getDocumentCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = query.getCondition().map(DocumentQueryConversor::convert).orElse(EMPTY);
        return stream(collection.find(mongoDBQuery).spliterator(), false).map(MongoDBUtils::of)
                .map(ds -> DocumentEntity.of(collectionName, ds)).collect(toList());

    }

    @Override
    public void close() {

    }


}
