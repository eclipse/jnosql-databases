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
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import org.jnosql.diana.api.document.Document;
import org.jnosql.diana.api.document.DocumentDeleteQuery;
import org.jnosql.diana.api.document.DocumentEntity;
import org.jnosql.diana.api.document.DocumentQuery;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.RID_FIELD;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.VERSION_FIELD;
import static org.jnosql.diana.orientdb.document.OrientDBConverter.toMap;

class DefaultOrientDBDocumentCollectionManager implements OrientDBDocumentCollectionManager {

    private final ODatabasePool pool;

    DefaultOrientDBDocumentCollectionManager(ODatabasePool pool) {
        this.pool = pool;
    }

    @Override
    public DocumentEntity insert(DocumentEntity entity) {
        requireNonNull(entity, "Entity is required");
        try (ODatabaseSession tx = pool.acquire()) {
            ODocument document = new ODocument(entity.getName());
            toMap(entity).forEach(document::field);
            try {
                tx.save(document);
            } catch (ONeedRetryException e) {
                document = tx.reload(document, OFetchHelper.DEFAULT, false);
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
        QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(selectQuery);

        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(orientQuery.getQuery(), orientQuery.getParams())) {
            while (resultSet.hasNext()) {
                OResult next = resultSet.next();
                tx.delete(next.toElement().getIdentity());
            }
        }

    }


    @Override
    public List<DocumentEntity> select(DocumentQuery query) {
        requireNonNull(query, "query is required");
        QueryOSQLFactory.QueryResult orientQuery = QueryOSQLFactory.to(query);

        try (ODatabaseSession tx = pool.acquire()) {
            List<DocumentEntity> entities = new ArrayList<>();
            if (orientQuery.isRunQuery()) {
                try (OResultSet resultSet = tx.command(orientQuery.getQuery(), orientQuery.getParams())) {
                    entities.addAll(OrientDBConverter.convert(resultSet));
                }
            }
            if(orientQuery.isLoad()) {
                orientQuery.getIds().stream().map(tx::load)
                        .map(o -> OrientDBConverter.convert((ODocument) o))
                        .forEach(entities::add);
            }
            return entities;
        }
    }

    @Override
    public long count(String documentCollection) {
        requireNonNull(documentCollection, "query is required");
        try (ODatabaseSession tx = pool.acquire()) {
            String query = "select count(*) from ".concat(documentCollection);
            OResultSet command = tx.command(query);
            OResult next = command.next();
            Object count = next.getProperty("count(*)");
            return Number.class.cast(count).longValue();

        }
    }

    @Override
    public List<DocumentEntity> sql(String query, Object... params) {
        requireNonNull(query, "query is required");
        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(query, params)) {
            return OrientDBConverter.convert(resultSet);
        }

    }

    @Override
    public List<DocumentEntity> sql(String query, Map<String, Object> params) {
        requireNonNull(query, "query is required");
        requireNonNull(params, "params is required");

        try (ODatabaseSession tx = pool.acquire();
             OResultSet resultSet = tx.command(query, params)) {
            return OrientDBConverter.convert(resultSet);

        }
    }

    @Override
    public void live(DocumentQuery query, OrientDBLiveCallback<DocumentEntity> callbacks) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        try (ODatabaseSession tx = pool.acquire();) {
            QueryOSQLFactory.QueryResult queryResult = QueryOSQLFactory.toLive(query, callbacks);
//            tx.command(queryResult.getQuery()).execute(queryResult.getParams());
        }
    }

    @Override
    public void live(String query, OrientDBLiveCallback<DocumentEntity> callbacks, Object... params) {
        requireNonNull(query, "query is required");
        requireNonNull(callbacks, "callbacks is required");
        try (ODatabaseSession tx = pool.acquire()) {
            OLiveQuery<ODocument> liveQuery = new OLiveQuery<>(query, new LiveQueryLIstener(callbacks));
//            tx.command(liveQuery).execute(params);
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
