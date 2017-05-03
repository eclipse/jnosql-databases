/*
 * Copyright 2017 Otavio Santana and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package org.jnosql.diana.mongodb.document;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.document.Documents;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static java.util.stream.StreamSupport.stream;
import static org.jnosql.diana.mongodb.document.MongoDBUtils.getDocument;

/**
 * The mongodb implementation to {@link DocumentCollectionManager} that does not support TTL methods
 * <p>{@link MongoDBDocumentCollectionManager#save(DocumentEntity, Duration)}</p>
 */
public class MongoDBDocumentCollectionManager implements DocumentCollectionManager {

    private static final String ID_FIELD = "_id";

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
        String collectionName = query.getCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = DocumentQueryConversor.convert(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("condition is required")));
        DeleteResult deleteResult = collection.deleteMany(mongoDBQuery);
    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        String collectionName = query.getCollection();
        MongoCollection<Document> collection = mongoDatabase.getCollection(collectionName);
        Bson mongoDBQuery = DocumentQueryConversor.convert(query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("condition is required")));
        return stream(collection.find(mongoDBQuery).spliterator(), false).map(this::of)
                .map(ds -> DocumentEntity.of(collectionName, ds)).collect(toList());

    }


    private List<org.jnosql.diana.api.document.Document> of(Map<String, ?> values) {
        Predicate<String> isNotNull = s -> values.get(s) != null;
        Function<String, org.jnosql.diana.api.document.Document> documentMap = key -> {
            Object value = values.get(key);
            if (value instanceof Document) {
                return org.jnosql.diana.api.document.Document.of(key, of(Document.class.cast(value)));
            } else if (isDocumentIterable(value)) {
                return org.jnosql.diana.api.document.Document.of(key, stream(Iterable.class.cast(value)
                        .spliterator(), false)
                        .flatMap(d -> of((Document) d).stream())
                        .collect(Collectors.toList()));
            }
            return org.jnosql.diana.api.document.Document.of(key, Value.of(value));
        };
        return values.keySet().stream().filter(isNotNull).map(documentMap).collect(Collectors.toList());
    }

    private boolean isDocumentIterable(Object value) {
        return value instanceof Iterable &&
                stream(Iterable.class.cast(value).spliterator(), false)
                        .allMatch(v -> Document.class.isInstance(v));
    }

    @Override
    public void close() {

    }


}
