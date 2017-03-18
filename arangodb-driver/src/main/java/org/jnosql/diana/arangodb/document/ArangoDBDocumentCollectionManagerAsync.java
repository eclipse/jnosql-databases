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
 * <p>{@link DocumentCollectionManagerAsync#save(DocumentEntity, Duration)}</p>
 * <p>{@link DocumentCollectionManagerAsync#save(DocumentEntity, Duration, Consumer)}</p>
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
    public void save(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, v -> {
        });
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }


    @Override
    public void save(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException
            , UnsupportedOperationException {

        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentCreateEntity<BaseDocument>> future = arangoDBAsync.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        future.thenAccept(d -> callBack.accept(entity));
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
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

        String collection = query.getCollection();
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
    public void find(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
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
    public void close() {

    }

    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }


}
