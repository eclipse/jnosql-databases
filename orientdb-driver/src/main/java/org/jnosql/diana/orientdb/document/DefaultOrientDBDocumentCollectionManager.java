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
 *   Lucas Furlaneto
 */
package org.jnosql.diana.orientdb.document;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.VERSION_FIELD;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.toMap;

class DefaultOrientDBDocumentCollectionManager implements OrientDBDocumentCollectionManager {

    private final OPartitionedDatabasePool pool;

    DefaultOrientDBDocumentCollectionManager(OPartitionedDatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        requireNonNull(entity, "Entity is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            ODocument document = new ODocument(entity.getName());
            toMap(entity).forEach(document::field);
            try {
                tx.save(document);
            } catch (ONeedRetryException e) {
                document = tx.reload(document);
                Map<String, Object> entityValues = toMap(entity);
                entityValues.put(OrientDBConverter.VERSION_FIELD, document.getVersion());
                entityValues.forEach(document::field);
                tx.save(document);
            }
            updateEntity(entity, document);
            return entity;
        }
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity, Duration ttl) {
        throw new UnsupportedOperationException("There is no support to ttl on OrientDB");
    }


    @Override
    public DocumentEntity update(DocumentEntity entity) {
        requireNonNull(entity, "Entity is required");
        return insert(entity);
    }

    @Override
    public void delete(DocumentDeleteQuery query) {
        requireNonNull(query, "query is required");
        DocumentQuery selectQuery = new OrientDBDocumentQuery(query);

        try (ODatabaseDocumentTx tx = pool.acquire()) {
            QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(selectQuery);
            List<ODocument> result = tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
            result.forEach(tx::delete);
        }

    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        requireNonNull(query, "query is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(query);
            List<ODocument> result = tx.command(orientQuery.getQuery()).execute(orientQuery.getParams());
            return OrientDBConverter.convert(result);
        }
    }

    @Override
    public long count(String documentCollection) {
        requireNonNull(documentCollection, "query is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            String query = "select count(*) from ".concat(documentCollection);
            List<ODocument> result = tx.command(QueryOSQLFactory.parse(query)).execute();
            return result.stream()
                    .findFirst()
                    .map(e -> e.field("count"))
                    .map(n -> Number.class.cast(n).longValue())
                    .orElse(0L);

        }
    }

    @Override
    public List<DocumentEntity> sql(String query, Object... params) {
        requireNonNull(query, "query is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            List<ODocument> result = tx.command(QueryOSQLFactory.parse(query)).execute(params);
            return OrientDBConverter.convert(result);
        }

    }

    @Override
    public List<DocumentEntity> sql(String query, Map<String, Object> params) {
        requireNonNull(query, "query is required");
        requireNonNull(params, "params is required");

        try (ODatabaseDocumentTx tx = pool.acquire()) {
            List<ODocument> result = tx.command(QueryOSQLFactory.parse(query)).execute(params);
            return OrientDBConverter.convert(result);
        }
    }

    @Override
    public void live(DocumentQuery query, OrientDBLiveCallback<DocumentEntity> callbacks) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        try (ODatabaseDocumentTx tx = pool.acquire();) {
            QueryOSQLFactory.QueryResult queryResult = QueryOSQLFactory.toLive(query, callbacks);
            tx.command(queryResult.getQuery()).execute(queryResult.getParams());
        }
    }

    @Override
    public void live(String query, OrientDBLiveCallback<DocumentEntity> callbacks, Object... params) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        try (ODatabaseDocumentTx tx = pool.acquire()) {
            OLiveQuery<ODocument> liveQuery = new OLiveQuery<>(query, new LiveQueryLIstener(callbacks));
            tx.command(liveQuery).execute(params);
        }
    }

    @Override
    public void close() {
        pool.close();
    }

    private void updateEntity(DocumentEntity entity, ODocument save) {
        ORecordId ridField = new ORecordId(save.getIdentity());
        entity.add(Document.of(RID_FIELD, ridField.toString()));
        entity.add(Document.of(VERSION_FIELD, save.getVersion()));
    }
}
