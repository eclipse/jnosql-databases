/*
 *  Copyright (c) 2022 Contributors to the Eclipse Foundation
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

package org.eclipse.jnosql.databases.mongodb.communication;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.result.DeleteResult;
import jakarta.data.repository.Sort;
import org.eclipse.jnosql.communication.document.DocumentDeleteQuery;
import org.eclipse.jnosql.communication.document.DocumentEntity;
import org.eclipse.jnosql.communication.document.DocumentManager;
import org.eclipse.jnosql.communication.document.DocumentQuery;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.eclipse.jnosql.communication.document.Documents;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.eclipse.jnosql.databases.mongodb.communication.MongoDBUtils.ID_FIELD;
import static org.eclipse.jnosql.databases.mongodb.communication.MongoDBUtils.getDocument;

/**
 * The mongodb implementation to {@link DocumentManager} that does not support TTL methods
 * <p>{@link MongoDBDocumentManager#insert(DocumentEntity, Duration)}</p>
 */
public class MongoDBDocumentManager implements DocumentManager {

    private static final BsonDocument EMPTY = new BsonDocument();

    private final MongoDatabase mongoDatabase;

    private final String database;

    MongoDBDocumentManager(MongoDatabase mongoDatabase, String database) {
        this.mongoDatabase = mongoDatabase;
        this.database = database;
    }


    @Override
    public String name() {
        return database;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");
        String collectionName = entity.name();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document document = getDocument(entity);
        collection.insertOne(document);
        boolean hasNotId = entity.documents().stream()
                .map(org.eclipse.jnosql.communication.document.Document::name).noneMatch(k -> k.equals(ID_FIELD));
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
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::insert)
                .collect(toList());
    }

    @Override
    public Iterable<DocumentEntity> insert(Iterable<DocumentEntity> entities, Duration ttl) {
        Objects.requireNonNull(entities, "entities is required");
        Objects.requireNonNull(ttl, "ttl is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(e -> insert(e, ttl))
                .collect(toList());
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        Objects.requireNonNull(entity, "entity is required");

        DocumentEntity copy = entity.copy();
        String collectionName = entity.name();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Document id = copy.find(ID_FIELD)
                .map(d -> new Document(d.name(), d.value().get()))
                .orElseThrow(() -> new UnsupportedOperationException("To update this DocumentEntity " +
                        "the field `id` is required"));
        copy.remove(ID_FIELD);
        collection.findOneAndReplace(id, getDocument(entity));
        return entity;
    }

    @Override
    public Iterable<DocumentEntity> update(Iterable<DocumentEntity> entities) {
        Objects.requireNonNull(entities, "entities is required");
        return StreamSupport.stream(entities.spliterator(), false)
                .map(this::update)
                .collect(toList());
    }


    @Override
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");

        String collectionName = query.name();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = query.condition().map(DocumentQueryConversor::convert).orElse(EMPTY);
        collection.deleteMany(mongoDBQuery);
    }


    @Override
    public Stream<DocumentEntity> select(DocumentQuery query) {
        Objects.requireNonNull(query, "query is required");
        String collectionName = query.name();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = query.condition().map(DocumentQueryConversor::convert).orElse(EMPTY);

        FindIterable<Document> documents = collection.find(mongoDBQuery);
        documents.projection(Projections.include(query.documents()));
        if (query.skip() > 0) {
            documents.skip((int) query.skip());
        }

        if (query.limit() > 0) {
            documents.limit((int) query.limit());
        }

        query.sorts().stream().map(this::getSort).forEach(documents::sort);

        return stream(documents.spliterator(), false).map(MongoDBUtils::of)
                .map(ds -> DocumentEntity.of(collectionName, ds));

    }

    @Override
    public long count(String documentCollection) {
        Objects.requireNonNull(documentCollection, "documentCollection is required");
        MongoCollection<Document> collection = mongoDatabase.getCollection(documentCollection);
        return collection.countDocuments();
    }

    @Override
    public void close() {

    }

    /**
     * Removes all documents from the collection that match the given query filter.
     * If no documents match, the collection is not modified.
     *
     * @param collectionName the collection name
     * @param filter         the delete filter
     * @return the number of documents deleted.
     * @throws NullPointerException when filter or collectionName is null
     */
    public long delete(String collectionName, Bson filter) {
        Objects.requireNonNull(filter, "filter is required");
        Objects.requireNonNull(collectionName, "collectionName is required");

        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        DeleteResult result = collection.deleteMany(filter);
        return result.getDeletedCount();
    }

    /**
     * Aggregates documents according to the specified aggregation pipeline.
     *
     * @param collectionName the collection name
     * @param pipeline the aggregation pipeline
     * @return the number of documents deleted.
     * @throws NullPointerException when filter or collectionName is null
     */
    public Stream<Map<String, BsonValue>> aggregate(String collectionName, List<Bson> pipeline) {
        Objects.requireNonNull(pipeline, "filter is required");
        Objects.requireNonNull(collectionName, "collectionName is required");
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        AggregateIterable<Document> aggregate = collection.aggregate(pipeline);
        return stream(aggregate.spliterator(), false)
                .map(Document::toBsonDocument);
    }

    /**
     * Finds all documents in the collection.
     *
     * @param collectionName the collection name
     * @param filter         the query filter
     * @return the stream result
     * @throws NullPointerException when filter or collectionName is null
     */
    public Stream<DocumentEntity> select(String collectionName, Bson filter) {
        Objects.requireNonNull(filter, "filter is required");
        Objects.requireNonNull(collectionName, "collectionName is required");
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        FindIterable<Document> documents = collection.find(filter);
        return stream(documents.spliterator(), false).map(MongoDBUtils::of)
                .map(ds -> DocumentEntity.of(collectionName, ds));
    }
    private Bson getSort(Sort sort) {
        return sort.isAscending() ? Sorts.ascending(sort.property()) : Sorts.descending(sort.property());
    }

}
