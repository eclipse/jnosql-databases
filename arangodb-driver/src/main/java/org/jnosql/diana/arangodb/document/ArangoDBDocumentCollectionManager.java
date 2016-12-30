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
import com.arangodb.entity.BaseDocument;
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

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;


public class ArangoDBDocumentCollectionManager implements DocumentCollectionManager {


    private static final String KEY_NAME = "";
    private static final Predicate<Document> FIND_KEY_DOCUMENT = d -> KEY_NAME.equals(d.getName());

    private final String database;

    private final ArangoDB arangoDB;
    private final ValueWriter writerField = ValueWriterDecorator.getInstance();

    ArangoDBDocumentCollectionManager(String database, ArangoDB arangoDB) {
        this.database = database;
        this.arangoDB = arangoDB;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity) throws NullPointerException {
        String collectionName = entity.getName();
        arangoDB.db(database).createCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        arangoDB.db(database).collection(collectionName).insertDocument(baseDocument);
        return entity;
    }


    @Override
    public void saveAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }


    @Override
    public void saveAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        String collectionName = entity.getName();
        arangoDB.db(database).createCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        arangoDB.db(database).collection(collectionName).updateDocument(baseDocument.getKey(), baseDocument);
        return entity;
    }

    @Override
    public void updateAsync(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void updateAsync(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void delete(DocumentQuery query) {
        String collection = query.getCollection();
        if (query.getConditions().isEmpty()) {
            return;
        }
        DocumentCondition documentCondition = query.getConditions().get(0);
        Value value = documentCondition.getDocument().getValue();
        if (Condition.IN.equals(documentCondition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {});
            arangoDB.db(database).collection(collection).deleteDocuments(keys);
        } else if (Condition.IN.equals(documentCondition.getCondition())) {
            String key = value.get(String.class);
            arangoDB.db(database).collection(collection).deleteDocument(key);
        }

    }

    @Override
    public void deleteAsync(DocumentQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void deleteAsync(DocumentQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public List<DocumentEntity> find(DocumentQuery query) throws NullPointerException {

        if (query.getConditions().isEmpty()) {
            return Collections.emptyList();
        }
        DocumentCondition documentCondition = query.getConditions().get(0);
        String key = documentCondition.getDocument().getValue().get(String.class);
        String collection = query.getCollection();
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        Map<String, Object> properties = document.getProperties();
        List<Document> documents = properties.keySet().stream()
                .map(k -> Document.of(k, properties.get(k)))
                .collect(Collectors.toList());
        documents.add(Document.of(KEY_NAME, document.getKey()));
        return singletonList(DocumentEntity.of(collection, documents));
    }

    @Override
    public void findAsync(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {

    }

    @Override
    public void close() {
        arangoDB.shutdown();
    }

    private Object convert(Value value) {
        Object val = value.get();
        if (writerField.isCompatible(val.getClass())) {
            return writerField.write(val);
        }
        return val;
    }

    private BaseDocument getBaseDocument(DocumentEntity entity) {
        BaseDocument baseDocument = new BaseDocument();

        baseDocument.setKey(entity.getDocuments().stream()
                .filter(FIND_KEY_DOCUMENT).findFirst()
                .map(d -> d.getValue().get(String.class))
                .orElseThrow(() -> new ArangoDBException("The entity must have a entity key")));
        entity.getDocuments().stream()
                .filter(FIND_KEY_DOCUMENT.negate())
                .forEach(d -> baseDocument.addAttribute(d.getName(),
                        convert(d.getValue())));
        return baseDocument;
    }

    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void saveAsync(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }
}
