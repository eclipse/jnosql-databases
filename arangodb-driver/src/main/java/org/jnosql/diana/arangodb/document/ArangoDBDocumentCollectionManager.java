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
package org.jnosql.diana.arangodb.document;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentDeleteEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import com.arangodb.entity.MultiDocumentEntity;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManager;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.writer.ValueWriterDecorator;
import org.jnosql.diana.arangodb.util.ArangoDBUtil;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;


public class ArangoDBDocumentCollectionManager implements DocumentCollectionManager {


    public static final String KEY = "_key";
    public static final String ID = "_id";
    public static final String REV = "_rev";
    private final String database;

    private final ArangoDB arangoDB;

    private final ArangoDBAsync arangoDBAsync;

    private final ValueWriter writerField = ValueWriterDecorator.getInstance();

    ArangoDBDocumentCollectionManager(String database, ArangoDB arangoDB, ArangoDBAsync arangoDBAsync) {
        this.database = database;
        this.arangoDB = arangoDB;
        this.arangoDBAsync = arangoDBAsync;
    }


    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        DocumentCreateEntity<BaseDocument> arandoDocument = arangoDB.db(database).collection(collectionName).insertDocument(baseDocument);
        if (!entity.find(KEY).isPresent()) {
            entity.add(Document.of(KEY, arandoDocument.getKey()));
        }
        if (!entity.find(ID).isPresent()) {
            entity.add(Document.of(ID, arandoDocument.getId()));
        }
        if (!entity.find(REV).isPresent()) {
            entity.add(Document.of(REV, arandoDocument.getRev()));
        }

        return entity;
    }

    @Override
    public DocumentEntity update(DocumentEntity entity) {
        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        arangoDB.db(database).collection(collectionName).updateDocument(baseDocument.getKey(), baseDocument);
        return entity;
    }

    @Override
    public void delete(DocumentQuery query) {
        String collection = query.getCollection();
        if (checkCondition(query)) {
            return;
        }
        DocumentCondition condition = query.getCondition();
        Value value = condition.getDocument().getValue();
        if (Condition.IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            arangoDB.db(database).collection(collection).deleteDocuments(keys);
        } else if (Condition.EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            arangoDB.db(database).collection(collection).deleteDocument(key);
        }

    }


    @Override
    public void saveAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        saveAsync(entity, v -> {
        });
    }


    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException
            , UnsupportedOperationException {

        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentCreateEntity<BaseDocument>> future = arangoDBAsync.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        future.thenAccept(d -> callBack.accept(entity));
    }

    @Override
    public void updateAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        updateAsync(entity, v -> {
        });
    }

    @Override
    public void updateAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentUpdateEntity<BaseDocument>> future = arangoDBAsync.db(database).collection(collectionName)
                .updateDocument(baseDocument.getKey(), baseDocument);
        future.thenAccept(d -> callBack.accept(entity));
    }


    @Override
    public void deleteAsync(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        deleteAsync(query, v -> {
        });
    }

    @Override
    public void deleteAsync(DocumentQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        String collection = query.getCollection();
        if (checkCondition(query)) {
            return;
        }
        DocumentCondition condition = query.getCondition();
        Value value = condition.getDocument().getValue();
        if (Condition.IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            CompletableFuture<MultiDocumentEntity<DocumentDeleteEntity<Void>>> future = arangoDBAsync.db(database)
                    .collection(collection).deleteDocuments(keys);
            future.thenAccept(d -> callBack.accept(null));
        } else if (Condition.IN.equals(condition.getCondition())) {
            String key = value.get(String.class);
            CompletableFuture<DocumentDeleteEntity<Void>> future = arangoDBAsync.db(database).
                    collection(collection).deleteDocument(key);
            future.thenAccept(d -> callBack.accept(null));
        }

    }

    @Override
    public void findAsync(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        if (checkCondition(query)) {
            callBack.accept(Collections.emptyList());
            return;
        }
        DocumentCondition condition = query.getCondition();
        Value value = condition.getDocument().getValue();
        String collection = query.getCollection();
        if (Condition.EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            CompletableFuture<BaseDocument> future = arangoDBAsync.db(database).collection(collection)
                    .getDocument(key, BaseDocument.class);

            future.thenAccept(d -> callBack.accept(singletonList(toEntity(collection, d))));

            return;
        }
        if (Condition.IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            List<DocumentEntity> entities = synchronizedList(new ArrayList<>());

            if (keys.isEmpty()) {
                callBack.accept(Collections.emptyList());
                return;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (String key : keys) {
                CompletableFuture<BaseDocument> future = arangoDBAsync.db(database).collection(collection)
                        .getDocument(key, BaseDocument.class);
                futures.add(future.thenAcceptAsync(d -> entities.add(toEntity(collection, d))));
            }
            CompletableFuture.allOf(futures.toArray(
                    new CompletableFuture[futures.size()]))
                    .thenAcceptAsync(v -> callBack.accept(entities));

        }

        callBack.accept(Collections.emptyList());

    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {

        if (checkCondition(query)) {
            return Collections.emptyList();
        }
        DocumentCondition condition = query.getCondition();
        Value value = condition.getDocument().getValue();
        String collection = query.getCollection();
        if (Condition.EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            DocumentEntity entity = toEntity(collection, key);
            if (Objects.isNull(entity)) {
                return Collections.emptyList();
            }
            return singletonList(entity);
        }
        if (Condition.IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            return keys.stream().map(k -> toEntity(collection, k))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }


    @Override
    public void close() {

    }

    private Object convert(Value value) {
        Object val = value.get();
        if (writerField.isCompatible(val.getClass())) {
            return writerField.write(val);
        }
        return val;
    }

    private BaseDocument getBaseDocument(DocumentEntity entity) {
        return new BaseDocument(entity.toMap());
    }

    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }

    private boolean checkCondition(DocumentQuery query) {
        if (Objects.isNull(query.getCondition())) {
            return true;
        }
        return false;
    }

    private DocumentEntity toEntity(String collection, String key) {
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        if (Objects.isNull(document)) {
            return null;
        }
        return toEntity(collection, document);
    }

    private DocumentEntity toEntity(String collection, BaseDocument document) {
        Map<String, Object> properties = document.getProperties();
        List<Document> documents = properties.keySet().stream()
                .map(k -> Document.of(k, properties.get(k)))
                .collect(Collectors.toList());
        documents.add(Document.of(KEY, document.getKey()));
        documents.add(Document.of(ID, document.getId()));
        documents.add(Document.of(REV, document.getRevision()));
        return DocumentEntity.of(collection, documents);
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }
}
