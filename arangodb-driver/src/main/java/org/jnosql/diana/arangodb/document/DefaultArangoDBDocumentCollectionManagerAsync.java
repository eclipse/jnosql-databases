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

import com.arangodb.ArangoCursorAsync;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBAsync;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.DocumentUpdateEntity;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import jakarta.nosql.document.Document;
import jakarta.nosql.document.DocumentDeleteQuery;
import jakarta.nosql.document.DocumentEntity;
import jakarta.nosql.document.DocumentQuery;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.ID;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.KEY;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.REV;
import static org.jnosql.diana.arangodb.document.ArangoDBUtil.getBaseDocument;

public class DefaultArangoDBDocumentCollectionManagerAsync implements ArangoDBDocumentCollectionManagerAsync {

    private static final Consumer NOOP = v -> {
    };

    private final ArangoDB arangoDB;

    private final ArangoDBAsync arangoDBAsync;

    private final String database;

    DefaultArangoDBDocumentCollectionManagerAsync(String database, ArangoDB arangoDB, ArangoDBAsync arangoDBAsync) {
        this.arangoDB = arangoDB;
        this.arangoDBAsync = arangoDBAsync;
        this.database = database;
    }

    @Override
    public void insert(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        insert(entity, NOOP);
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }


    @Override
    public void insert(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException
            , UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");

        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentCreateEntity<BaseDocument>> future = arangoDBAsync.db(database)
                .collection(collectionName).insertDocument(baseDocument);
        future.thenAccept(d -> createConsumer(entity, callBack, d.getKey(), d.getId(), d.getRev()));
    }

    @Override
    public void insert(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("TTL is not supported on ArangoDB implementation");
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        update(entity, NOOP);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(entity, "entity is required");
        requireNonNull(callBack, "callBack is required");

        String collectionName = entity.getName();
        checkCollection(collectionName);
        BaseDocument baseDocument = getBaseDocument(entity);
        CompletableFuture<DocumentUpdateEntity<BaseDocument>> future = arangoDBAsync.db(database).collection(collectionName)
                .updateDocument(baseDocument.getKey(), baseDocument);
        future.thenAccept(d -> createConsumer(entity, callBack, d.getKey(), d.getId(), d.getRev()));
    }


    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, NOOP);
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        AQLQueryResult delete = QueryAQLConverter.delete(query);
        CompletableFuture<ArangoCursorAsync<BaseDocument>> future = arangoDBAsync.db(database).query(delete.getQuery(), delete.getValues(),
                null, BaseDocument.class);
        future.thenAccept(c -> callBack.accept(null));
    }


    @Override
    public void select(DocumentQuery query, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException, UnsupportedOperationException {

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");

        AQLQueryResult result = QueryAQLConverter.select(query);
        runAql(result.getQuery(), result.getValues(), callBack);


    }

    @Override
    public void count(String documentCollection, Consumer<Long> callback) {
        Objects.requireNonNull(documentCollection, "document collection is required");
        Objects.requireNonNull(callback, "callback is required");
        String aql = "RETURN LENGTH(" + documentCollection + ")";
        CompletableFuture<ArangoCursorAsync<Object>> query = arangoDBAsync.db(database).query(aql, Collections.emptyMap(),
                null, Object.class);
        query.thenAccept(r -> {
            long count = StreamSupport.stream(r.spliterator(), false).findFirst().map(Long.class::cast).orElse(0L);
            callback.accept(count);
        });

    }


    @Override
    public void aql(String query, Map<String, Object> values, Consumer<List<DocumentEntity>> callBack)
            throws ExecuteAsyncQueryException,
            UnsupportedOperationException, NullPointerException {

        requireNonNull(query, "query is required");
        requireNonNull(callBack, "callBack is required");
        requireNonNull(values, "values is required");

        runAql(query, values, callBack);

    }

    private void runAql(String query, Map<String, Object> values, Consumer<List<DocumentEntity>> callBack) {
        CompletableFuture<ArangoCursorAsync<BaseDocument>> future = arangoDBAsync.db(database).query(query,
                values, null, BaseDocument.class);

        future.thenAccept(b -> {
            List<DocumentEntity> entities = StreamSupport.stream(b.spliterator(), false).map(ArangoDBUtil::toEntity).collect(toList());
            callBack.accept(entities);
        });
    }


    @Override
    public void close() {

    }

    private void checkCollection(String collectionName) {
        ArangoDBUtil.checkCollection(database, arangoDB, collectionName);
    }

    private void createConsumer(DocumentEntity entity, Consumer<DocumentEntity> callBack, String key, String id, String rev) {
        entity.add(Document.of(KEY, key));
        entity.add(Document.of(ID, id));
        entity.add(Document.of(REV, rev));
        callBack.accept(entity);
    }


}
