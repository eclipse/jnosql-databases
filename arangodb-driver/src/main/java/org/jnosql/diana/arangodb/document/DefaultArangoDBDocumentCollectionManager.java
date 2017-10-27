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

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.StreamSupport;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.api.Condition.EQUALS;
import static org.jnosql.diana.api.Condition.IN;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;

class DefaultArangoDBDocumentCollectionManager implements ArangoDBDocumentCollectionManager {


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
        requireNonNull(query, "query is required");
        String collection = query.getDocumentCollection();
        if (checkCondition(query.getCondition())) {
            return;
        }
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        if (IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            arangoDB.db(database).collection(collection).deleteDocuments(keys);
        } else if (EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            arangoDB.db(database).collection(collection).deleteDocument(key);
        }

    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) throws NullPointerException {
        requireNonNull(query, "query is required");
        AQLQueryResult result = AQLUtils.convert(query);

        if (isJustKey(query)) {
            return findByKeys(query);
        }

        ArangoCursor<BaseDocument> documents = arangoDB.db(database).query(result.getQuery(),
                result.getValues(), null, BaseDocument.class);

        return StreamSupport.stream(documents.spliterator(), false)
                .map(ArangoDBUtil::toEntity)
                .collect(toList());
    }

    private List<DocumentEntity> findByKeys(DocumentQuery query) {
        DocumentCondition condition = query.getCondition().get();
        Value value = condition.getDocument().getValue();
        String collection = query.getDocumentCollection();
        if (EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            DocumentEntity entity = toEntity(collection, key);
            if (Objects.isNull(entity)) {
                return Collections.emptyList();
            }
            return singletonList(entity);
        } else if (IN.equals(condition.getCondition())) {
            List<String> keys = value.get(new TypeReference<List<String>>() {
            });
            return keys.stream().map(k -> toEntity(collection, k))
                    .collect(toList());
        }

        return Collections.emptyList();
    }


    private boolean isJustKey(DocumentQuery query) {
        if (query.getCondition().isPresent()) {
            DocumentCondition condition = query.getCondition().get();
            boolean isKeyDocument = KEY.equals(condition.getDocument().getName());
            boolean isEqualsKeys = EQUALS.equals(condition.getCondition()) && isKeyDocument;
            boolean isINKeys = IN.equals(condition.getCondition()) && isKeyDocument;
            return isEqualsKeys || isINKeys;
        }
        return false;
    }

    private DocumentEntity toEntity(String collection, String key) {
        BaseDocument document = arangoDB.db(database).collection(collection).getDocument(key, BaseDocument.class);
        if (Objects.isNull(document)) {
            return null;
        }
        return ArangoDBUtil.toEntity(document);
    }

    @Override
    public List<DocumentEntity> aql(String query, Map<String, Object> values) throws NullPointerException {
        requireNonNull(query, "query is required");
        requireNonNull(values, "values is required");

        ArangoCursor<BaseDocument> result = arangoDB.db(database).query(query, values, null, BaseDocument.class);
        return StreamSupport.stream(result.spliterator(), false)
                .map(ArangoDBUtil::toEntity)
                .collect(toList());

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


    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }


}
