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
import org.jnosql.diana.api.Condition;
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
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;


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
        return ArangoDBUtil.toEntity(collection, document);
    }


    @Override
    public DocumentEntity save(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

}
