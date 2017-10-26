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
package org.jnosql.diana.arangodb.document;

import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import org.jnosql.diana.api.Condition;
import org.jnosql.diana.api.TypeReference;
import org.jnosql.diana.api.Value;
import org.jnosql.diana.api.ValueWriter;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;
import org.jnosql.diana.api.writer.ValueWriterDecorator;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;

class DefaultArangoDBDocumentCollectionManager implements ArangoDBDocumentCollectionManager{


    public static final String KEY = "_key";
    public static final String ID = "_id";
    public static final String REV = "_rev";
    private final String database;

    private final ArangoDB arangoDB;

    private final ValueWriter writerField = ValueWriterDecorator.getInstance();

    DefaultArangoDBDocumentCollectionManager(String database, ArangoDB arangoDB) {
        this.database = database;
        this.arangoDB = arangoDB;
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity) throws NullPointerException {
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
    public void delete(DocumentDeleteQuery query) {
        Objects.requireNonNull(query, "query is required");
        String collection = query.getDocumentCollection();
        if (checkCondition(query.getCondition())) {
            return;
        }
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
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
    public List<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        Objects.requireNonNull(query, "query is required");
        if (checkCondition(query.getCondition())) {
            return Collections.emptyList();
        }
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        String collection = query.getDocumentCollection();
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

    private boolean checkCondition(Optional<DocumentCondition> query) {
        return !query.isPresent();
    }

    private DocumentEntity toEntity(String collection, String key) {
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        if (Objects.isNull(document)) {
            return null;
        }
        return ArangoDBUtil.toEntity(collection, document);
    }


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }
}
