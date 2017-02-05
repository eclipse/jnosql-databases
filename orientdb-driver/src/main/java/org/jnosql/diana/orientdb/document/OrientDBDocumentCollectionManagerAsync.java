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
package org.jnosql.diana.orientdb.document;


import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.ORecordCallback;
import org.jnosql.diana.api.ExecuteAsyncQueryException;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentCollectionManagerAsync;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import static com.orientechnologies.orient.core.db.ODatabase.OPERATION_MODE.ASYNCHRONOUS;
import static java.util.stream.Collectors.toList;
import static org.jnosql.diana.orientdb.document.OSQLQueryFactory.toAsync;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;

/**
 * The OrientDB implementation to {@link DocumentCollectionManagerAsync} this method does not support TTL method:
 * <p> {@link OrientDBDocumentCollectionManagerAsync#save(DocumentEntity, Duration)}</p>
 * <p>{@link OrientDBDocumentCollectionManagerAsync#save(DocumentEntity, Duration, Consumer)}</p>
 * Also has supports to query:
 * <p>{@link OrientDBDocumentCollectionManagerAsync#find(String, Consumer, Object...)}</p>
 */
public class OrientDBDocumentCollectionManagerAsync implements DocumentCollectionManagerAsync {

    private static final Consumer<DocumentEntity> NOOPS = d -> {
    };


    private final OPartitionedDatabasePool pool;

    OrientDBDocumentCollectionManagerAsync(OPartitionedDatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("There is support to ttl on OrientDB");
    }

    @Override
    public void save(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, NOOPS);
    }

    @Override
    public void save(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        Objects.toString(entity, "Entity is required");
        ODatabaseDocumentTx tx = pool.acquire();
        ODocument document = new ODocument(entity.getName());
        Map<String, Object> entityValues = entity.toMap();
        entityValues.keySet().stream().forEach(k -> document.field(k, entityValues.get(k)));
        ORecordCallback<Number> createCallBack = (a, b) -> {
            entity.add(Document.of(RID_FIELD, a.toString()));
            callBack.accept(entity);
        };
        ORecordCallback<Integer> updateCallback = (a, b) -> {
            entity.add(Document.of(RID_FIELD, a.toString()));
            callBack.accept(entity);
        };
        tx.save(document, null, ASYNCHRONOUS, false, createCallBack, updateCallback);
    }

    @Override
    public void save(DocumentEntity entity, Duration ttl, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        throw new UnsupportedOperationException("There is support to ttl on OrientDB");
    }

    @Override
    public void update(DocumentEntity entity) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity);
    }

    @Override
    public void update(DocumentEntity entity, Consumer<DocumentEntity> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        save(entity, callBack);
    }

    @Override
    public void delete(DocumentDeleteQuery query) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        delete(query, v -> {
        });
    }

    @Override
    public void delete(DocumentDeleteQuery query, Consumer<Void> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        ODatabaseDocumentTx tx = pool.acquire();
        OSQLQueryFactory.QueryResult orientQuery = toAsync(DocumentQuery.of(query.getCollection())
                .and(query.getCondition().orElseThrow(() -> new IllegalArgumentException("Condition is required"))), l -> {
                l.forEach(d -> d.delete());
                callBack.accept(null);
            });
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    @Override
    public void find(DocumentQuery query, Consumer<List<DocumentEntity>> callBack) throws ExecuteAsyncQueryException, UnsupportedOperationException {
        ODatabaseDocumentTx tx = pool.acquire();
        OSQLQueryFactory.QueryResult orientQuery = toAsync(query, l -> {
            callBack.accept(l.stream()
                    .map(OrientDBConverter::convert)
                    .collect(toList()));
        });
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }

    public void find(String query, Consumer<List<DocumentEntity>> callBack, Object... params) {
        ODatabaseDocumentTx tx = pool.acquire();
        OSQLQueryFactory.QueryResult orientQuery = toAsync(query, l -> {
            callBack.accept(l.stream()
                    .map(OrientDBConverter::convert)
                    .collect(toList()));
        }, params);
        tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
    }


    @Override
    public void close() {
        pool.close();
    }
}
