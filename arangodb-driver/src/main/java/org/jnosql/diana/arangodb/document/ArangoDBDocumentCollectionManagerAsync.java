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
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentCondition;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static java.util.Collections.singletonList;
import static java.util.Collections.synchronizedList;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.checkCondition;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.toEntity;

/**
 * The ArandoDB implementation of {@link DocumentCollectionManagerAsync}. It does not support to TTL methods:
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, Duration)}</p>
 * <p>{@link DocumentCollectionManagerAsync#insert(DocumentEntity, Duration, Consumer)}</p>
 */
public class ArangoDBDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {


    private final ArangoDB arangoDB;

    private final ArangoDBAsync arangoDBAsync;

    private final String database;

    ArangoDBDocumentCollectionManagerAsync(String database, ArangoDB arangoDB, ArangoDBAsync arangoDBAsync) {
        this.arangoDB = arangoDB;
        this.arangoDBAsync = arangoDBAsync;
        this.database = database;
    }

    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, v -> {
        });
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }


    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException
            , UnsupportedOperationException {

        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentCreateEntity<BaseDocument>> future = arangoDBAsync.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        future.thenAccept(d -> callBack.accept(entity));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        update(entity, v -> {
        });
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {
        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentUpdateEntity<BaseDocument>> future = arangoDBAsync.db(database).collection(collectionName)
                .updateDocument(baseDocument.getKey(), baseDocument);
        future.thenAccept(d -> callBack.accept(entity));
    }


    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, v -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBack, "callBack is required");

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
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        Objects.requireNonNull(query, "query is required");
        Objects.requireNonNull(callBack, "callBack is required");
        if (checkCondition(query.getCondition())) {
            callBack.accept(Collections.emptyList());
            return;
        }
        DocumentCondition condition = query.getCondition()
                .orElseThrow(() -> new IllegalArgumentException("Condition is required"));
        Value value = condition.getDocument().getValue();
        String collection = query.getDocumentCollection();
        if (Condition.EQUALS.equals(condition.getCondition())) {
            String key = value.get(String.class);
            CompletableFuture<BaseDocument> future = arangoDBAsync.db(database).collection(collection)
                    .getDocument(key, BaseDocument.class);

            future.thenAccept(d -> callBack.accept(singletonList(toEntity(d))));

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
                futures.add(future.thenAcceptAsync(d -> entities.add(toEntity(d))));
            }
            CompletableFuture.allOf(futures.toArray(
                    new CompletableFuture[futures.size()]))
                    .thenAcceptAsync(v -> callBack.accept(entities));

        }

        callBack.accept(Collections.emptyList());

    }


    @Override
    public void close() {

    }

    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }


}
